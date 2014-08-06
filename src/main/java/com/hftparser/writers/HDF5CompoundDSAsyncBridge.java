package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

import java.util.concurrent.Executor;

/**
 * Created by patrick on 8/6/14.
 */
public class HDF5CompoundDSAsyncBridge<T> extends HDF5CompoundDSCachingBridge<T> {
    private Executor executor;
    private Writer lastWriter;

    public class Writer implements Runnable {
        boolean isDone = false;

        @Override
        public void run() {
            System.out.println("Flushing.");
            decoratedBridge.flush();
            isDone = true;
        }

        public boolean isDone() {
            return isDone;
        }
    }

    public Writer getLastWriter() {
        return lastWriter;
    }

    public HDF5CompoundDSAsyncBridge(HDF5CompoundDSCachingBridge<T> decoratedBridge, Executor executor) {
        this.decoratedBridge = decoratedBridge;
        this.executor = executor;
        lastWriter = null;
    }


    private void waitForLastWriter() {
        //noinspection StatementWithEmptyBody
        while ((lastWriter != null) && (!lastWriter.isDone())) { }
    }

    @Override
    public void appendElement(T element) {
        System.out.println("Async append element");
        decoratedBridge.appendElement(element);
    }

    @Override
    protected void doFlush() {
        System.out.println("Called async doFlush");
        waitForLastWriter();
        lastWriter = new Writer();

        executor.execute(lastWriter);
    }

    @Override
    public void flush() {
        doFlush();
//        waitForLastWriter();
//        decoratedBridge.flush();
    }

    @Override
    public T[] fixUp() {
        return decoratedBridge.fixUp();
    }

    @Override
    public HDF5GenericStorageFeatures initFeatures(HDF5CompoundDSBridgeConfig bridgeConfig) {
        return decoratedBridge.initFeatures(bridgeConfig);
    }

    @Override
    public T[] readBlock(long offset, int blocksize) {
        return decoratedBridge.readBlock(offset, blocksize);
    }

    @Override
    public T[] readBlock(long offset) {
        return decoratedBridge.readBlock(offset);
    }

    @Override
    public T[] readArray() {
        return decoratedBridge.readArray();
    }
}
