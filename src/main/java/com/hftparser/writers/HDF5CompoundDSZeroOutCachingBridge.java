package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

/**
 * Created by patrick on 8/4/14.
 */
public class HDF5CompoundDSZeroOutCachingBridge<T> extends HDF5CompoundDSCachingBridge<T> {

    public HDF5CompoundDSZeroOutCachingBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer,
                                              long startSize, int chunkSize, HDF5CompoundDSBridgeConfig bridgeConfig) {
        super(name, type, writer, startSize, chunkSize, bridgeConfig);
    }

    @Override
    protected T[] fixUp() {
        zeroOutExtra();
        return cache;
    }

    private void zeroOutExtra() {
        //        System.out.println("Flushing. Current offset: " + currentCacheOffset + ".");
        for (int i = currentCacheOffset; i < cache.length; i++) {
            //            System.out.println("Zeroing out " + i);

            cache[i] = emptyPoint;
        }
    }
}
