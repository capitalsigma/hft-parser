package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import org.jetbrains.annotations.NotNull;

/**
 * Created by patrick on 7/28/14.
 */
class HDF5CompoundDSBridgeBuilder<T> {
    private HDF5CompoundType<T> type;
    private IHDF5CompoundWriter writer;
    private long startSize;
    private int chunkSize;
    private HDF5CompoundDSBridgeConfig bridgeConfig;

    public HDF5CompoundType<T> getType() {
        return type;
    }

    public void setTypeFromInferred(Class<T> typeToInferClass) {
        type = writer.getInferredType(typeToInferClass);
    }

    public long getStartSize() {
        return startSize;
    }

    public void setStartSize(long startSize) {
        this.startSize = startSize;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }


    public HDF5CompoundDSBridgeBuilder(IHDF5Writer writer) {
        this(writer, HDF5CompoundDSBridgeConfig.getDefault());
    }

    HDF5CompoundDSBridgeBuilder(IHDF5Writer writer, HDF5CompoundDSBridgeConfig bridgeConfig) {
        this.bridgeConfig = bridgeConfig;
        this.writer = writer.compound();
    }

    public HDF5CompoundDSBridge<T> build(@NotNull DatasetName name) throws HDF5FormatNotFoundException {
        if (type == null || writer == null) {
            throw new HDF5FormatNotFoundException();
        } else {
            return new HDF5CompoundDSBridge<>(name, type, writer, startSize, chunkSize, bridgeConfig);
        }

    }
}
