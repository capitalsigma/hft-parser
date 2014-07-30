package com.hftparser.main;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.hftparser.containers.WaitFreeQueue;
import com.hftparser.readers.ArcaParser;
import com.hftparser.readers.DataPoint;
import com.hftparser.readers.GzipReader;
import com.hftparser.writers.HDF5Writer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ParseRun {
    private static int QUEUE_SIZE = 10;

    private static class Args {
        @Parameter
        private List<String> parameters = new ArrayList<String>();

        @Parameter(names = {"-symbols", "-s"}, description = "CSV containing symbols")
        private String symbolPath;

        @Parameter(names = {"-book ", "-b"}, description = "Gzipped CSV of book data")
        private String bookPath;

        @Parameter(names = {"-out", "-o"}, description = "Output .h5 file")
        private String outPath;
    }


    public static void main(String[] argv) {
        Args args = new Args();
        new JCommander(args, argv);
        String[] symbols;
        InputStream GzipInstream;
        GzipReader gzipReader;
        ArcaParser parser;
        HDF5Writer writer;
        File outFile;
        Thread readerThread;
        Thread parserThread;
        Thread writerThread;

        Thread[] allThreads;

        if(args.bookPath == null || args.outPath == null) {
            System.out.println("Book path and output path must be specified.");
            return;
        }

        if(args.symbolPath == null){
            symbols = new String[]{
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
                System.out.println("Got error opening symbol file:" + e.toString());
                System.out.println("Exiting.");
                return;
            }
        }

        try {
            GzipInstream = new FileInputStream(new File(args.bookPath));
        } catch (FileNotFoundException e) {
            System.out.println("Error opening book file.");
            System.out.println("Exiting");
            return;
        }


        outFile = new File(args.outPath);
        WaitFreeQueue<String> linesReadQueue = new WaitFreeQueue<>(QUEUE_SIZE);
        WaitFreeQueue<DataPoint> dataPointQueue = new WaitFreeQueue<>(QUEUE_SIZE);

        try {
            gzipReader = new GzipReader(GzipInstream, linesReadQueue);
        } catch(IOException e) {
            System.out.println("Error opening book file for reading.");
            return;
        }

        parser = new ArcaParser(symbols, linesReadQueue, dataPointQueue);
        writer = new HDF5Writer(dataPointQueue, outFile);

        readerThread = new Thread(gzipReader);
        parserThread = new Thread(parser);
        writerThread = new Thread(writer);

        allThreads = new Thread[] {
                readerThread,
                parserThread,
                writerThread
        };

        System.out.println("Starting parser.");
        for(Thread t : allThreads){
            t.start();
        }
        System.out.println("Parser started. Waiting.");

        try {
            for (Thread t : allThreads) {
                t.join();
            }
        } catch (InterruptedException e) {
            System.out.println("A thread was ended prematurely. Stopping");
            return;
        }

        System.out.println("Successfully created" + args.outPath);
    }

    private static String[] parseSymbolFile(File symbolFile)
            throws IOException {
        ArrayList<String> ret = new ArrayList<String>();
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(symbolFile));

        while((line = reader.readLine()) != null) {
            ret.add(line.trim());
        }
        reader.close();

        return ret.toArray(new String[ret.size()]);
    }

}