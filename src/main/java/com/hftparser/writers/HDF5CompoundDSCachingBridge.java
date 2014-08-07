package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

/**
 * Created by patrick on 7/31/14.
 */
public abstract class HDF5CompoundDSCachingBridge<T> extends HDF5CompoundDSBridge<T> {
    protected final T[] cache;
    protected final int cacheSize;
    protected int currentCacheOffset;
//    private final static WritableDataPoint emptyPoint = new WritableDataPoint(new int[][]{}, new int[][]{}, 0, 0l);
protected final T emptyPoint;

    protected HDF5CompoundDSCachingBridge() {
        cache = null;
        cacheSize = -1;
        emptyPoint = null;
    }

    public HDF5CompoundDSCachingBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer,
                                long startSize, int chunkSize, HDF5CompoundDSBridgeConfig bridgeConfig){
        super(name, type, writer, startSize, chunkSize, bridgeConfig);
        cacheSize = bridgeConfig.getCache_size();

        System.out.println("Building cache of size:" + cacheSize);

        //noinspection unchecked
        cache = (T[])  new Object[cacheSize];

//        grab the default value to use as out "blank out" later
        emptyPoint = readBlock(0)[0];

//        System.out.println("Built caching");
    }

    @Override
    public void appendElement(T element) {
        cache[currentCacheOffset++] = element;

        if (currentCacheOffset >= cacheSize) {
            System.out.println("About to call doFlush");
            doFlush();
        }
    }

//    split this off so we can override
    protected void doFlush() {
        flush();
    }

    @Override
    public void flush() {
        System.out.println("Called flush");
        //    template method
        T[] toWrite = fixUp();

        writer.writeArrayBlockWithOffset(fullPath, type, toWrite, currentOffset);
        currentOffset += currentCacheOffset;
        currentCacheOffset = 0;
    }

    abstract protected T[] fixUp();
}
