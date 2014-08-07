package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

import java.util.Arrays;
import java.util.concurrent.Executor;

/**
 * Created by patrick on 8/6/14.
 */
public class HDF5CompoundDSAsyncCutoffBridge<T> extends HDF5CompoundDSAsyncBridge<T> {
    // TODO: this is really ugly. really, the cuttoff/zero-out should be a decorator,
    // and async should be extension. ideally it would just use multiple inheritance, but we can't. oh well,
    // let's just get it done for now


    public HDF5CompoundDSAsyncCutoffBridge(DatasetName name,
                                           HDF5CompoundType<T> type,
                                           IHDF5CompoundWriter writer,
                                           long startSize,
                                           int chunkSize,
                                           HDF5CompoundDSBridgeConfig bridgeConfig,
                                           Executor executor) {
        super(name, type, writer, startSize, chunkSize, bridgeConfig, executor);
    }

    @Override
    protected T[] fixUp() {
        return cutoffExtra();
    }

    private T[] cutoffExtra() {
        if (currentCacheOffset == cache.length) {
            return cache;
        } else {
            return Arrays.copyOfRange(cache, 0, currentCacheOffset);
        }
    }
}
