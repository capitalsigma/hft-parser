package com.hftparser.main;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.hftparser.config.*;
import com.hftparser.containers.Backoffable;
import com.hftparser.containers.WaitFreeQueue;
import com.hftparser.readers.ArcaParser;
import com.hftparser.readers.DataPoint;
import com.hftparser.readers.GzipReader;
import com.hftparser.readers.MarketOrderCollectionFactory;
import com.hftparser.writers.HDF5Writer;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class InterruptedRunException extends Exception {
    public InterruptedRunException() {
    }
}

// WARNING: TURNING ON ASSERTIONS BREAKS SOMETHING INSIDE OF THE CISD CODE
// NEVER, EVER, EVER TURN ON ASSERTIONS

// NOTE: COMPACT data layout is not extensible.
public class ParseRun {
    private static int LINE_QUEUE_SIZE;
    private static int POINT_QUEUE_SIZE;
    private static WaitFreeQueue<String> linesReadQueue;
    private static WaitFreeQueue<DataPoint> dataPointQueue;
    private static Backoffable sBackoffOne;
    private static Backoffable sBackoffTwo;
    private static Backoffable dBackoffOne;
    private static Backoffable dBackoffTwo;
    private static MarketOrderCollectionConfig marketOrderCollectionConfig;
    private static MarketOrderCollectionFactory orderCollectionFactory;
    private static ArcaParserConfig arcaParserConfig;
    private static File outFile;
    private static HDF5WriterConfig hdf5WriterConfig;
    private static HDF5CompoundDSBridgeConfig hdf5CompoundDSBridgeConfig;
    private static Calendar startCalendar;
    private static InputStream gzipInstream;
    private static String bookPath;

    //    private static int MIN_BACKOFF;
//    private static int MAX_BACKOFF;

    private static class Args {
        @Parameter
        private List<String> parameters = new ArrayList<>();

        @Parameter(names = {"-symbols", "-s"}, description = "CSV containing symbols")
        private String symbolPath;

        @Parameter(names = {"-book ", "-b"}, description = "Gzipped CSV of book data")
        private String bookPath;

        @Parameter(names = {"-out", "-o"}, description = "Output .h5 file")
        private String outPath;

        @Parameter(names = {"-config", "-c"}, description = "JSON-formatted config file")
        private String configPath;

        @Parameter(names = {"-num", "-n"}, description = "Number of lines per run")
        private Integer numPerRun;

        //        @Parameter(names = {"-stats", "-s"}, description = "Output file for run statistics")
        //        private String statsPath;
    }


    public static void main(String[] argv) {
        Args args = new Args();
        new JCommander(args, argv);
        String[] symbols = null;
        ArcaParser parser;
        HDF5Writer writer;
        Thread readerThread;
        Thread parserThread;
        Thread writerThread;
        Thread[] allThreads;
        ConfigFactory configFactory = null;

        long startTime = System.currentTimeMillis();
        long endTime;

        if(args.bookPath == null || args.outPath == null || args.configPath == null) {
            System.out.println("Book path and output path must be specified.");
            return;
        }

        if(args.symbolPath == null){
            symbols = new String[] {
                "SPY", "DIA", "QQQ",
                "XLK", "XLF", "XLP", "XLE", "XLY", "XLV", "XLB",
                "VCR", "VDC", "VHT", "VIS", "VAW", "VNQ", "VGT", "VOX", "VPU",
                "XOM", "RDS", "BP",
                "HD", "LOW", "XHB",
                "MS", "GS", "BAC", "JPM", "C",
                "CME", "NYX",
                "AAPL", "MSFT", "GOOG", "CSCO",
                "GE", "CVX", "JNJ", "IBM", "PG", "PFE",
            };
        } else {
            File symbolFile = new File(args.symbolPath);
            try {
                symbols = parseSymbolFile(symbolFile);
            } catch (IOException e) {
                printErrAndExit("Got error opening symbol file:" + e.toString());
            }
        }
        System.out.println("Running with symbols: " + Arrays.deepToString(symbols));

        bookPath = args.bookPath;
        outFile = new File(args.outPath);

        try {
            configFactory = ConfigFactory.fromPath(args.configPath);
        } catch (BadConfigFileError | IOException e) {
            printErrAndExit("There was a problem loading your config file: " + e.toString());
        }


        @SuppressWarnings("ConstantConditions")
        ParseRunConfig parseRunConfig = configFactory.getParseRunConfig();
        setProperties(parseRunConfig);

        sBackoffOne = parseRunConfig.makeBackoffFor(ParseRunConfig.BackoffType.String);
        sBackoffTwo = parseRunConfig.makeBackoffFor(ParseRunConfig.BackoffType.String);

        dBackoffOne = parseRunConfig.makeBackoffFor(ParseRunConfig.BackoffType.DataPoint);
        dBackoffTwo = parseRunConfig.makeBackoffFor(ParseRunConfig.BackoffType.DataPoint);

        marketOrderCollectionConfig = configFactory.getMarketOrderCollectionConfig();
        orderCollectionFactory = new MarketOrderCollectionFactory(marketOrderCollectionConfig);
        arcaParserConfig = configFactory.getArcaParserConfig();
        hdf5WriterConfig = configFactory.getHdf5WriterConfig();
        hdf5CompoundDSBridgeConfig = configFactory.getHdf5CompoundDSBridgeConfig();
        startCalendar = startCalendarFromFilename(args.bookPath);


        if (args.numPerRun == null) {
            runLoop(symbols, symbols.length);
        } else {
            runLoop(symbols, args.numPerRun);
        }


        endTime = System.currentTimeMillis();
        System.out.println("Successfully created " + args.outPath);
        printTotalTime(startTime, endTime);
    }

    private static void initQueues() {
        linesReadQueue = new WaitFreeQueue<>(LINE_QUEUE_SIZE, sBackoffOne, sBackoffTwo);
        dataPointQueue = new WaitFreeQueue<>(POINT_QUEUE_SIZE, dBackoffOne, dBackoffTwo);
    }

    private static void openGzipInstream() {
        try {
            gzipInstream = new FileInputStream(new File(bookPath));
        } catch (FileNotFoundException e) {
            printErrAndExit("Error opening book file.");
        }
    }

    public static void runLoop(String[] allSymbols,
                               @NotNull
                               Integer numPerRun) {
        String[] symbolsForThisRun;
        HDF5Writer previousWriter = null;
        try {
            for (int i = 0; i < allSymbols.length; i += numPerRun) {
                symbolsForThisRun = Arrays.copyOfRange(allSymbols, i, Math.min(i + numPerRun, allSymbols.length));
                previousWriter = runForSymbols(symbolsForThisRun, i != 0);
            }

            if(previousWriter != null) {
                previousWriter.closeFile();
            }

        } catch (InterruptedRunException e) {
            System.out.println("Run was interrupted early. Quitting.");
        }
    }

    public static HDF5Writer runForSymbols(String[] symbols) {
        try {
            return runForSymbols(symbols, false);
        } catch (InterruptedRunException e) {
            System.out.println("Run was interrupted early. Quitting.");
//            we've already done all the error handling we can do, just fail
            return null;
        }
    }

    public static HDF5Writer runForSymbols(String[] symbols, boolean preserveForNextRun)
            throws InterruptedRunException {
        GzipReader gzipReader = null;
        ArcaParser parser;
        HDF5Writer writer;
        Thread readerThread;
        Thread parserThread;
        Thread writerThread;
        Thread[] allThreads;

        System.out.println("Running on symbol subset:" + Arrays.deepToString(symbols));

        long startTime = System.currentTimeMillis();
        long endTime;

        openGzipInstream();
        initQueues();

        try {
            gzipReader = new GzipReader(gzipInstream, linesReadQueue);
        } catch (IOException e) {
            printErrAndExit("Error opening book file for reading: " + e.toString());
        }

        if (preserveForNextRun) {
            hdf5WriterConfig.setOverwrite(false);
        }

        parser = new ArcaParser(symbols, linesReadQueue, dataPointQueue, orderCollectionFactory, arcaParserConfig);
        writer = new HDF5Writer(dataPointQueue, outFile, hdf5WriterConfig, hdf5CompoundDSBridgeConfig);

        writer.setCloseFileAtEnd(false);


        if (startCalendar != null) {
            parser.setStartCalendar(startCalendar);
        }

        readerThread = new Thread(gzipReader);
        parserThread = new Thread(parser);
        writerThread = new Thread(writer);

        allThreads = new Thread[]{readerThread, parserThread, writerThread};

        System.out.println("Starting parser.");
        for (Thread t : allThreads) {
            t.start();
        }
        System.out.println("Parser started. Waiting.");

        try {
            for (Thread t : allThreads) {
                t.join();
            }
        } catch (Exception e) {
            System.out.println("A thread threw an exception:" + e.toString());
            System.out.println("Stopping.");
            writer.closeFile();
            throw new InterruptedRunException();
        } finally {
            endTime = System.currentTimeMillis();
            printRunTime(startTime, endTime);
            printQueueUsage();
        }
        return writer;
    }

    private static void printQueueUsage() {
        System.out.println("Information for String queue:");
        linesReadQueue.printUsage();
        System.out.println("Information for Datapoint queue:");
        dataPointQueue.printUsage();
    }


    public static Calendar startCalendarFromFilename(String bookPath) {
//        format is YYYYMMDD
        String datePattern = "\\D+(\\d{4})(\\d{2})(\\d{2})\\.csv\\.gz";
        Pattern dateRe = Pattern.compile(datePattern);
        Matcher matcher = dateRe.matcher(bookPath);
        Calendar startDate;
        System.out.println("Trying to match timestamp.");
        if (matcher.matches()) {
            System.out.println("Got match on: " + matcher.group(0));
            //            System.out.println("Group 0:" + matcher.group(0));

            startDate = Calendar.getInstance();

            //            clear, otherwise it holds on to the current hour, second, etc
            startDate.clear();


            //            note that months are 0-based (i.e. january == 0)
            //noinspection MagicConstant
            startDate.set(Integer.valueOf(matcher.group(1)),
                          Integer.valueOf(matcher.group(2)) - 1,
                          Integer.valueOf(matcher.group(3)));

            return startDate;
        }
        System.out.println("No match.");

        return null;
    }

    private static void printTime(long startMs, long endMs, String segmentMsg) {
        double diff = endMs - startMs;

        System.out.printf("%s time: %.3f sec\n", segmentMsg, diff / 1000);
    }

    private static void printRunTime(long startMs, long endMs) {
        printTime(startMs, endMs, "Run");
    }

    private static void printTotalTime(long startMs, long endMs) {
        printTime(startMs, endMs, "Total");
    }

    private static void setProperties(ParseRunConfig config) {
        LINE_QUEUE_SIZE = config.getLine_queue_size();
        POINT_QUEUE_SIZE = config.getPoint_queue_size();


    }

    private static void printErrAndExit(String err) {
        System.out.println(err);
        System.out.println("Exiting");
        System.exit(1);
    }

    public static @NotNull String[] parseSymbolFile(File symbolFile)
            throws IOException {
        ArrayList<String> ret = new ArrayList<>();
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(symbolFile));

        while((line = reader.readLine()) != null) {
            ret.add(line.trim());
        }
        reader.close();

        return ret.toArray(new String[ret.size()]);
    }

}