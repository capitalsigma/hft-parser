package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

/**
 * Created by patrick on 7/31/14.
 */
public class HDF5CompoundDSCachingBridge<T> extends HDF5CompoundDSBridge<T> {
    private final T[] cache;
    private final int cacheSize;
    private int currentCacheOffset;

    public HDF5CompoundDSCachingBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer,
                                long startSize, int chunkSize, HDF5CompoundDSBridgeConfig bridgeConfig){
        super(name, type, writer, startSize, chunkSize, bridgeConfig);
        cacheSize = bridgeConfig.getCache_size();
        cache = (T[])  new Object[cacheSize];
    }

}
