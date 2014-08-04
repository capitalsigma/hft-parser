package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;

/**
 * Created by patrick on 8/4/14.
 */
public class HDF5CompoundDSReadOnlyBridge<T> {
    protected final IHDF5CompoundWriter writer;
    protected final String fullPath;
    protected final HDF5CompoundType<T> type;

    public HDF5CompoundDSReadOnlyBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer) {
        fullPath = name.getFullPath();
        this.writer = writer;
        this.type = type;

    }

    T[] readBlock(long offset, int blocksize) {
        return writer.readArrayBlockWithOffset(fullPath, type, blocksize, offset);
    }

    public T[] readBlock(long offset){
        return readBlock(offset, 1);
    }

    public T[] readArray() {
        return writer.readArray(fullPath, type);
    }

}
