package com.hftparser.writers;

import java.io.File;
import java.util.HashMap;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import com.hftparser.containers.WaitFreeQueue;
import com.hftparser.readers.DataPoint;
import com.hftparser.readers.WritableDataPoint;


public class HDF5Writer implements Runnable {
	HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>> dsForTicker;
    HDF5CompoundDSBridgeBuilder<WritableDataPoint> bridgeBuilder;
	IHDF5Writer fileWriter;
	WaitFreeQueue<DataPoint> inQueue;

    int START_SIZE = 10;
    int CHUNK_SIZE = 10;

    String BOOK_DS_NAME = "books";

	// Here we try to open up a new HDF5 file in the path we've been
	// given, and raise abstraction-appropriate exceptions if
	// something goes wrong.
	public HDF5Writer(WaitFreeQueue<DataPoint> _inQueue,
					  File outFile) {

        fileWriter = getDefaultWriter(outFile);

        bridgeBuilder = new HDF5CompoundDSBridgeBuilder<WritableDataPoint>(fileWriter);

        bridgeBuilder.setStartSize(START_SIZE);
        bridgeBuilder.setChunkSize(CHUNK_SIZE);
        bridgeBuilder.setTypeFromInferred(WritableDataPoint.class);

        // TODO: we know how big it is, we should inst. appropriately
		dsForTicker = new HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>>();

        inQueue = _inQueue;
	}

    private void writePoint(DataPoint dataPoint){
        String ticker = dataPoint.getTicker();
        HDF5CompoundDSBridge<WritableDataPoint> bridge;

        if((bridge = dsForTicker.get(ticker)) == null){
            DatasetName name = new DatasetName(ticker, BOOK_DS_NAME);
            bridge = bridgeBuilder.build(name);

            dsForTicker.put(ticker, bridge);
        }

        bridge.appendElement(dataPoint.getWritable());
    }

    public void run(){
        DataPoint dataPoint;

        try {
            while (inQueue.acceptingOrders || !inQueue.isEmpty()) {
                if ((dataPoint = inQueue.deq()) != null) {
                    writePoint(dataPoint);
                }
            }
        } finally {
            fileWriter.close();
        }
    }

    public static IHDF5Writer getDefaultWriter(File file) {
        IHDF5WriterConfigurator config = HDF5Factory.configure(file);
        config.keepDataSetsIfTheyExist();
        config.overwrite();
        config.syncMode(IHDF5WriterConfigurator.SyncMode.SYNC_BLOCK);
        config.performNumericConversions();
        return config.writer();
    }
}
