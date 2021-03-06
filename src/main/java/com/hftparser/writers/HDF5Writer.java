package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.config.HDF5WriterConfig;
import com.hftparser.containers.WaitFreeQueue;
import com.hftparser.data.DataPoint;
import com.hftparser.data.DataSetName;
import com.hftparser.data.PoisonDataPointException;
import com.hftparser.data.WritableDataPoint;
import org.apache.commons.lang.mutable.MutableBoolean;

import java.io.File;
import java.util.HashMap;


public class HDF5Writer implements Runnable {
    private final HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>> dsForTicker;
    private final IHDF5Writer fileWriter;
    private final WaitFreeQueue<DataPoint> inQueue;
    private final MutableBoolean pipelineError;
    private final int START_SIZE;
    // following FLUSH_FREQ from the original python
    private final int CHUNK_SIZE;
    private final String BOOK_DS_NAME = "books";
    private boolean closeFileAtEnd;
    private HDF5CompoundDSBridgeBuilder<WritableDataPoint> bridgeBuilder;

    // Here we try to open up a new HDF5 file in the path we've been
    // given, and raise abstraction-appropriate exceptions if
    // something goes wrong.
    public HDF5Writer(WaitFreeQueue<DataPoint> _inQueue,
                      File outFile,
                      HDF5WriterConfig writerConfig,
                      HDF5CompoundDSBridgeConfig bridgeConfig,
                      MutableBoolean pipelineError) {

        fileWriter = getWriter(outFile, writerConfig);
        START_SIZE = writerConfig.getStart_size();
        CHUNK_SIZE = writerConfig.getChunk_size();
        this.pipelineError = pipelineError;

        bridgeBuilder = new HDF5CompoundDSBridgeBuilder<>(fileWriter, bridgeConfig);

        bridgeBuilder.setStartSize(START_SIZE);
        bridgeBuilder.setChunkSize(CHUNK_SIZE);
        bridgeBuilder.setTypeFromInferred(WritableDataPoint.class);

        dsForTicker = new HashMap<>();

        inQueue = _inQueue;
        closeFileAtEnd = false;

    }

    public HDF5Writer(WaitFreeQueue<DataPoint> _inQueue, File outFile) {
        this(_inQueue,
             outFile,
             HDF5WriterConfig.getDefault(),
             HDF5CompoundDSBridgeConfig.getDefault(),
             new MutableBoolean());
    }


    public HDF5Writer(WaitFreeQueue<DataPoint> _inQueue, File outFile, MutableBoolean pipelineError) {
        this(_inQueue, outFile, HDF5WriterConfig.getDefault(), HDF5CompoundDSBridgeConfig.getDefault(), pipelineError);
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

    public void setBridgeBuilder(HDF5CompoundDSBridgeBuilder<WritableDataPoint> bridgeBuilder) {
        this.bridgeBuilder = bridgeBuilder;
    }

    private HDF5CompoundDSBridge<WritableDataPoint> getDSBridgeForTicker(String ticker) {
        HDF5CompoundDSBridge<WritableDataPoint> bridge;
        if ((bridge = dsForTicker.get(ticker)) == null) {
            DataSetName name = new DataSetName(ticker, BOOK_DS_NAME);
            bridge = bridgeBuilder.build(name);

            dsForTicker.put(ticker, bridge);
        }

        return bridge;
    }

    private void writePoint(DataPoint dataPoint) throws HDF5CompoundDSBridge.FailedWriteError {
        HDF5CompoundDSBridge<WritableDataPoint> bridge = getDSBridgeForTicker(dataPoint.getTicker());

        try {
            bridge.appendElement(dataPoint.getWritable());
        } catch (PoisonDataPointException ex) {
            //            Now we'll be deleted at the end
            bridge.poison();
        }
    }

    public boolean isCloseFileAtEnd() {
        return closeFileAtEnd;
    }

    public void setCloseFileAtEnd(boolean closeFileAtEnd) {
        this.closeFileAtEnd = closeFileAtEnd;
    }

    public void run() {
        DataPoint dataPoint;

        // TODO: we aren't catching it correctly (and dying) when there is a low-level write error
        // (e.g. the file is clobberred by another run). why?

        try {
            while (!pipelineError.booleanValue() && (inQueue.acceptingOrders || !inQueue.isEmpty())) {
                if ((dataPoint = inQueue.deq()) != null) {
                    //                    System.out.println("Writer got a datapoint.");
                    writePoint(dataPoint);
                }
            }
        } catch (HDF5CompoundDSBridge.FailedWriteError error) {
            pipelineError.setValue(true);
            System.out.println("Writer failed. Stack trace:");
            error.printStackTrace();
        } finally {
            for (HDF5CompoundDSBridge<WritableDataPoint> bridge : dsForTicker.values()) {
                try {
                    bridge.flush(fileWriter);
                } catch (HDF5CompoundDSBridge.FailedWriteError failedWriteError) {
                    pipelineError.setValue(true);
                    System.out.println("Failed trying to close existing file handles.");
                    failedWriteError.printStackTrace();
                }
            }

            if (closeFileAtEnd) {
                fileWriter.close();
            }
        }
    }

    public void closeFile() {
        fileWriter.close();
    }

    public IHDF5Writer getFileWriter() {
        return fileWriter;
    }

    public HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>> getDsForTicker() {
        return dsForTicker;
    }


}
