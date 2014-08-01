package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.readers.WritableDataPoint;

/**
 * Created by patrick on 7/31/14.
 */
public class HDF5CompoundDSCachingBridge<T> extends HDF5CompoundDSBridge<T> {
    private final T[] cache;
    private final int cacheSize;
    private int currentCacheOffset;
//    private final static WritableDataPoint emptyPoint = new WritableDataPoint(new int[][]{}, new int[][]{}, 0, 0l);
    private final T emptyPoint;

    public HDF5CompoundDSCachingBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer,
                                long startSize, int chunkSize, HDF5CompoundDSBridgeConfig bridgeConfig){
        super(name, type, writer, startSize, chunkSize, bridgeConfig);
        cacheSize = bridgeConfig.getCache_size();
        cache = (T[])  new Object[cacheSize];

//        grab the default value to use as out "blank out" later
        emptyPoint = readBlock(0)[0];

//        System.out.println("Built caching");
    }

    @Override
    public void appendElement(T element) {
        cache[currentCacheOffset++] = element;

        if (currentCacheOffset >= cacheSize) {
            flush();
        }
    }

    @Override
    public void flush() {
        zeroOutExtra();

        writer.writeArrayBlockWithOffset(fullPath, type, cache, currentOffset);
        currentOffset += currentCacheOffset;
        currentCacheOffset = 0;
    }

    private void zeroOutExtra() {
//        System.out.println("Flushing. Current offset: " + currentCacheOffset + ".");
        for (int i = currentCacheOffset; i < cache.length; i++) {
//            System.out.println("Zeroing out " + i);

            cache[i] = emptyPoint;
        }
    }
}
