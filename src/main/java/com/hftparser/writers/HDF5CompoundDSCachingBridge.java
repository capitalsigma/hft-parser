package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.data.DataSetName;

/**
 * Created by patrick on 7/31/14.
 */
public class HDF5CompoundDSCachingBridge<T> extends HDF5CompoundDSBridge<T> {
    protected ElementCache<T> cache;


    protected HDF5CompoundDSCachingBridge() {
        cache = null;
    }

    public HDF5CompoundDSCachingBridge(DataSetName name,
                                       HDF5CompoundType<T> type,
                                       IHDF5CompoundWriter writer,
                                       long startSize,
                                       int chunkSize,
                                       HDF5CompoundDSBridgeConfig bridgeConfig,
                                       ElementCacheFactory<T> cacheFactory) {

        super(name, type, writer, startSize, chunkSize, bridgeConfig);
        cacheFactory.setEmptyElement(readBlock(0)[0]);
        cache = cacheFactory.getCache();
    }

    @Override
    public void appendElement(T element) throws HDF5CompoundDSAsyncBridge.FailedWriteError {
        cache.appendElement(element);

        if (cache.isFull()) {
            doFlush();
        }
    }

    @Override
    public void prepareFlush() throws HDF5CompoundDSAsyncBridge.FailedWriteError {
        flush(cache);
    }

    //    split this off so we can override
    protected void doFlush() throws HDF5CompoundDSAsyncBridge.FailedWriteError {
        prepareFlush();
    }

    public void flush(ElementCache<T> cacheToFlush) {
        //        System.out.println("Called prepareFlush");


        writer.writeArrayBlockWithOffset(fullPath, type, cacheToFlush.getElements(), currentOffset);
        currentOffset += cacheToFlush.getCurrentCacheOffset();
        cacheToFlush.resetCurrentCacheOffset();
    }

}
