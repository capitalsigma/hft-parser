package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import org.jetbrains.annotations.NotNull;

/**
 * Created by patrick on 7/28/14.
 */
public class HDF5CompoundDSBridgeBuilder<T> {
    private HDF5CompoundType<T> type;
    private final IHDF5CompoundWriter writer;
    private long startSize;
    private int chunkSize;
    private boolean cutoff;
    private final HDF5CompoundDSBridgeConfig bridgeConfig;

    public HDF5CompoundType<T> getType() {
        return type;
    }

    public void setTypeFromInferred(Class<T> typeToInferClass) {
        type = writer.getInferredType(typeToInferClass);
    }

    public void setTypeForDSPath(String dataSetPath, Class<T> typeToInferClass) {
        type = writer.getInferredType(dataSetPath, typeToInferClass);
    }

    public void setAnonTypeFromInferred(Class<T> typeToInferClass) {
        type = writer.getInferredAnonType(typeToInferClass);
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

    public void setCutoff(boolean cutoff) {
        this.cutoff = cutoff;
    }

    public HDF5CompoundDSBridgeBuilder(IHDF5Writer writer) {
        this(writer, HDF5CompoundDSBridgeConfig.getDefault());
    }

    HDF5CompoundDSBridgeBuilder(IHDF5Writer writer, HDF5CompoundDSBridgeConfig bridgeConfig) {
        this.bridgeConfig = bridgeConfig;
        this.writer = writer.compound();
        this.cutoff = bridgeConfig.isCutoff();
    }

    public HDF5CompoundDSBridge<T> build(@NotNull DatasetName name) throws HDF5FormatNotFoundException {
        if (type == null || writer == null) {
            throw new HDF5FormatNotFoundException();
        } else {
            if (bridgeConfig.getCache_size() > 0) {
                //                System.out.println("Building caching");

                return buildCaching(name);
            } else {
//                System.out.println("Building regular");
                return new HDF5CompoundDSBridge<>(name, type, writer, startSize, chunkSize, bridgeConfig);
            }
        }
    }

    public HDF5CompoundDSCachingBridge<T> buildCaching(@NotNull DatasetName name) throws HDF5FormatNotFoundException {
        if (cutoff) {
            return new HDF5CompoundDSCutoffCachingBridge<>(name, type, writer, startSize, chunkSize,
                                                           bridgeConfig);
        } else {
            return new HDF5CompoundDSZeroOutCachingBridge<>(name, type, writer, startSize, chunkSize,
                                                            bridgeConfig);
        }
    }

    public HDF5CompoundDSReadOnlyBridge<T> buildReadOnly(@NotNull DatasetName name) throws HDF5FormatNotFoundException {
        if (type == null || writer == null) {
            throw new HDF5FormatNotFoundException();
        }

        return new HDF5CompoundDSReadOnlyBridge<>(name, type, writer);
    }
}
