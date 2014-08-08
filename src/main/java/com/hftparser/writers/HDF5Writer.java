package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.config.HDF5WriterConfig;
import com.hftparser.containers.WaitFreeQueue;
import com.hftparser.readers.DataPoint;
import com.hftparser.readers.WritableDataPoint;

import java.io.File;
import java.util.HashMap;


public class HDF5Writer implements Runnable {
    private final HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>> dsForTicker;
    private final HDF5CompoundDSBridgeBuilder<WritableDataPoint> bridgeBuilder;
    private final IHDF5Writer fileWriter;
    private final WaitFreeQueue<DataPoint> inQueue;
    private boolean closeFileAtEnd;

    private final int START_SIZE;
    // following FLUSH_FREQ from the original python
    //      TODO: this is a BYTE SIZE, not a number of rows, and IT SHOULD BE A MULTIPLE OF THE ROW SIZE
    private final int CHUNK_SIZE;

    private final String BOOK_DS_NAME = "books";

    // Here we try to open up a new HDF5 file in the path we've been
    // given, and raise abstraction-appropriate exceptions if
    // something goes wrong.
    public HDF5Writer(WaitFreeQueue<DataPoint> _inQueue,
                      File outFile,
                      HDF5WriterConfig writerConfig,
                      HDF5CompoundDSBridgeConfig bridgeConfig) {

        fileWriter = getWriter(outFile, writerConfig);
        START_SIZE = writerConfig.getStart_size();
        CHUNK_SIZE = writerConfig.getChunk_size();

        bridgeBuilder = new HDF5CompoundDSBridgeBuilder<>(fileWriter, bridgeConfig);

        bridgeBuilder.setStartSize(START_SIZE);
        bridgeBuilder.setChunkSize(CHUNK_SIZE);
        bridgeBuilder.setTypeFromInferred(WritableDataPoint.class);

        // TODO: we know how big it is, we should inst. appropriately
        dsForTicker = new HashMap<>();

        inQueue = _inQueue;
        closeFileAtEnd = false;
    }


    public HDF5Writer(WaitFreeQueue<DataPoint> _inQueue, File outFile) {
        this(_inQueue, outFile, HDF5WriterConfig.getDefault(), HDF5CompoundDSBridgeConfig.getDefault());
    }

    private void writePoint(DataPoint dataPoint) {
        String ticker = dataPoint.getTicker();
        HDF5CompoundDSBridge<WritableDataPoint> bridge;

        if ((bridge = dsForTicker.get(ticker)) == null) {
            DatasetName name = new DatasetName(ticker, BOOK_DS_NAME);
            bridge = bridgeBuilder.build(name);

            dsForTicker.put(ticker, bridge);
        }

        bridge.appendElement(dataPoint.getWritable());
    }


    public boolean isCloseFileAtEnd() {
        return closeFileAtEnd;
    }

    public void setCloseFileAtEnd(boolean closeFileAtEnd) {
        this.closeFileAtEnd = closeFileAtEnd;
    }

    public void run() {
        DataPoint dataPoint;

        try {
            while (inQueue.acceptingOrders || !inQueue.isEmpty()) {
                if ((dataPoint = inQueue.deq()) != null) {
                    //                    System.out.println("Writer got a datapoint.");
                    writePoint(dataPoint);
                }
            }
        } finally {
            for (HDF5CompoundDSBridge<WritableDataPoint> bridge : dsForTicker.values()) {
                bridge.flush();
            }

            if (closeFileAtEnd) {
                fileWriter.close();
            }
        }
    }

    public void closeFile() {
        fileWriter.close();
    }


    private static IHDF5Writer getWriter(File file, HDF5WriterConfig writerConfig) {
        IHDF5WriterConfigurator config = HDF5Factory.configure(file);
        if (writerConfig.isKeep_datasets_if_they_exist()) {
            config.keepDataSetsIfTheyExist();
        }
        if (writerConfig.isOverwrite()) {
            config.overwrite();
        }

        if (writerConfig.isPerform_numeric_conversions()) {
            config.performNumericConversions();
        }

        config.syncMode(writerConfig.getSync_mode());


        return config.writer();
    }

    public static IHDF5Writer getWriter(File file) {
        return getWriter(file, HDF5WriterConfig.getDefault());
    }

    public IHDF5Writer getFileWriter() {
        return fileWriter;
    }

    public HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>> getDsForTicker() {
        return dsForTicker;
    }
}
