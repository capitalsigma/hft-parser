package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

import java.util.concurrent.Executor;

/**
 * Created by patrick on 8/6/14.
 */
public class HDF5CompoundDSAsyncBridge<T> extends HDF5CompoundDSCachingBridge<T> {
    private Executor executor;
    private Writer lastWriter;
    private ElementCache<T> cacheOne;
    private ElementCache<T> cacheTwo;

    public class Writer implements Runnable {
        volatile boolean isDone = false;
        ElementCache<T> cacheToWrite;

        public Writer(ElementCache<T> cacheToWrite) {
            this.cacheToWrite = cacheToWrite;
        }

        @Override
        public void run() {
            //            System.out.println("Flushing.");
            flush(cacheToWrite);
            isDone = true;
        }

        public boolean isDone() {
            return isDone;
        }
    }

    public HDF5CompoundDSAsyncBridge(DatasetName name,
                                     HDF5CompoundType<T> type,
                                     IHDF5CompoundWriter writer,
                                     long startSize,
                                     int chunkSize,
                                     HDF5CompoundDSBridgeConfig bridgeConfig,
                                     ElementCacheFactory<T> cacheFactory,
                                     Executor executor) {
        super(name, type, writer, startSize, chunkSize, bridgeConfig, cacheFactory);
        cacheOne = cache;
        cacheTwo = cacheFactory.getCache();
        this.executor = executor;
    }


    private void swapCaches() {
        if (cache == cacheOne) {
            cache = cacheTwo;
        } else {
            cache = cacheOne;
        }
    }

    public Writer getLastWriter() {
        return lastWriter;
    }

    private void waitForLastWriter() {
        //noinspection StatementWithEmptyBody
        while ((lastWriter != null) && (!lastWriter.isDone())) {
        }
    }

    @Override
    protected void doFlush() {
        //        System.out.println("Called async doFlush");
        waitForLastWriter();
        lastWriter = new Writer(cache);

        executor.execute(lastWriter);
        swapCaches();
    }

    @Override
    public void flush() {
        waitForLastWriter();
        super.flush();
    }
}
