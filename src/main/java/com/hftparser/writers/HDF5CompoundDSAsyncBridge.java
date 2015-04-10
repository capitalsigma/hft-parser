package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.data.DataSetName;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by patrick on 8/6/14.
 */
public class HDF5CompoundDSAsyncBridge<T> extends HDF5CompoundDSCachingBridge<T> {
    private final ExecutorService executor;
    private Future<?> lastWriter;
    private final ElementCache<T> cacheOne;
    private final ElementCache<T> cacheTwo;

    public class Writer implements Runnable {
        volatile boolean isDone = false;
        final ElementCache<T> cacheToWrite;

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

    public HDF5CompoundDSAsyncBridge(DataSetName name,
                                     HDF5CompoundType<T> type,
                                     IHDF5CompoundWriter writer,
                                     long startSize,
                                     int chunkSize,
                                     HDF5CompoundDSBridgeConfig bridgeConfig,
                                     ElementCacheFactory<T> cacheFactory,
                                     ExecutorService executor) {
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

    public Future<?> getLastWriter() {
        return lastWriter;
    }

    private void waitForLastWriter() throws FailedWriteError {
        //noinspection StatementWithEmptyBody
        //        while ((lastWriter != null) && (!lastWriter.isDone())) {
        //        }

        try {
            if (lastWriter != null) {
                lastWriter.get();
            }
        } catch (Throwable t) {
            throw new FailedWriteError(t);
        }
    }

    @Override
    protected void doFlush() throws FailedWriteError {
        //        System.out.println("Called async doFlush");
        waitForLastWriter();
        lastWriter = executor.submit(new Writer(cache));

        swapCaches();
    }

    @Override
    public void flush() throws FailedWriteError {
        waitForLastWriter();
        super.flush();
    }
}
