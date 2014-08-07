package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

import java.util.Arrays;

/**
 * Created by patrick on 8/4/14.
 */
public class HDF5CompoundDSCutoffCachingBridge<T> extends HDF5CompoundDSCachingBridge<T> {

    public HDF5CompoundDSCutoffCachingBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer,
                                             long startSize, int chunkSize, HDF5CompoundDSBridgeConfig bridgeConfig) {
        super(name, type, writer, startSize, chunkSize, bridgeConfig);
    }

    @Override
    protected T[] fixUp() {
        return cutoffExtra();
    }

    private T[] cutoffExtra() {

    }


}
