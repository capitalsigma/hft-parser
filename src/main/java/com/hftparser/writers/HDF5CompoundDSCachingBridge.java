package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

/**
 * Created by patrick on 7/31/14.
 */
public class HDF5CompoundDSCachingBridge<T> extends HDF5CompoundDSBridge<T> {
    protected ElementCache<T> cache;


    protected HDF5CompoundDSCachingBridge() {
        cache = null;
    }

    public HDF5CompoundDSCachingBridge(DatasetName name,
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
    public void appendElement(T element) {
        cache.appendElement(element);

        if (cache.isFull()){
            doFlush();
        }
    }

//    split this off so we can override
    protected void doFlush() {
        flush();
    }

    @Override
    public void flush() {
        flush(cache);
    }

    public void flush(ElementCache<T> cacheToFlush) {
//        System.out.println("Called flush");


        writer.writeArrayBlockWithOffset(fullPath, type, cacheToFlush.getElements(), currentOffset);
        currentOffset += cacheToFlush.getCurrentCacheOffset();
        cacheToFlush.resetCurrentCacheOffset();
    }
}
