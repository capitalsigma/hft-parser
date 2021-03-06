package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.data.DataSetName;

/**
 * Created by patrick on 7/28/14.
 */
class HDF5CompoundDSBridge<T> extends HDF5CompoundDSReadOnlyBridge<T> {
    private final T[] elToWrite;
    protected long currentOffset;
    private boolean poisoned;

    protected HDF5CompoundDSBridge() {
        elToWrite = null;
    }

    public HDF5CompoundDSBridge(DataSetName name,
                                HDF5CompoundType<T> type,
                                IHDF5CompoundWriter writer,
                                long startSize,
                                int chunkSize,
                                HDF5CompoundDSBridgeConfig bridgeConfig) {
        super(name, type, writer);

        HDF5GenericStorageFeatures features = initFeatures(bridgeConfig);

        writer.createArray(fullPath, type, startSize, chunkSize, features);
        currentOffset = 0;
        //noinspection unchecked
        elToWrite = (T[]) new Object[1];
    }

    protected HDF5GenericStorageFeatures initFeatures(HDF5CompoundDSBridgeConfig bridgeConfig) {
        //            System.out.println("Initialized with: " + bridgeConfig.toString());
        HDF5GenericStorageFeatures.HDF5GenericStorageFeatureBuilder featureBuilder = HDF5GenericStorageFeatures.build();

        //            if(bridgeConfig.isDefault_storage_layout()) {
        //                featureBuilder.defaultStorageLayout();
        //            }

        featureBuilder.storageLayout(bridgeConfig.getStorage_layout());
        featureBuilder.deflateLevel(bridgeConfig.getDeflate_level());
        //           TODO: is this necessary? I don't think so (unless we get duplicate types)
        featureBuilder.datasetReplacementEnforceKeepExisting();

        return featureBuilder.features();
    }

    public void appendElement(T element) throws FailedWriteError {
        elToWrite[0] = element;
        //            System.out.println("At offset: " + currentOffset);
        writer.writeArrayBlockWithOffset(fullPath, type, elToWrite, currentOffset++);
    }

    public void poison() {
        poisoned = true;
    }

    // No one else is allowed to call this -- it's supposed to be subclassed
    protected void prepareFlush() throws FailedWriteError {
        // Sublcasses are responsible for doing cleanup before we're called. So now it's safe to delete ourselves
    }

    //    We need to pass in a fileWriter here in case we've been marked for deletion
    public void flush(IHDF5Writer fileWriter) throws FailedWriteError {
        prepareFlush();

        if (poisoned) {
            fileWriter.delete(fullPath);
        }
    }

    public boolean isPoisoned() {
        return poisoned;
    }

    public static class FailedWriteError extends Exception {
        public FailedWriteError(Throwable cause) {
            super(cause);
        }
    }
}
