package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;

/**
 * Created by patrick on 7/28/14.
 */
class HDF5CompoundDSBridge<T> extends HDF5CompoundDSReadOnlyBridge<T> {
    protected long currentOffset;
//    protected final IHDF5CompoundWriter writer;
//    protected final String fullPath;
    private final T[] elToWrite;
//    protected final HDF5CompoundType<T> type;

    public HDF5CompoundDSBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer,
                                long startSize, int chunkSize, HDF5CompoundDSBridgeConfig bridgeConfig) {
        super(name, type, writer);

        HDF5GenericStorageFeatures features = initFeatures(bridgeConfig);

        writer.createArray(name.getFullPath(), type, startSize, chunkSize, features);
        currentOffset = 0;
        //noinspection unchecked
        elToWrite = (T[]) new Object[1];
    }

    protected HDF5GenericStorageFeatures initFeatures(HDF5CompoundDSBridgeConfig bridgeConfig) {
        //            System.out.println("Initialized with: " + bridgeConfig.toString());
        HDF5GenericStorageFeatures.HDF5GenericStorageFeatureBuilder featureBuilder = HDF5GenericStorageFeatures
                .build();

        //            if(bridgeConfig.isDefault_storage_layout()) {
        //                featureBuilder.defaultStorageLayout();
        //            }

        featureBuilder.storageLayout(bridgeConfig.getStorage_layout());
        featureBuilder.deflateLevel(bridgeConfig.getDeflate_level());
        //           TODO: is this necessary? I don't think so (unless we get duplicate types)
        featureBuilder.datasetReplacementEnforceKeepExisting();

        return featureBuilder.features();
    }

    public void appendElement(T element) {
        elToWrite[0] = element;
        //            System.out.println("At offset: " + currentOffset);
        writer.writeArrayBlockWithOffset(fullPath, type, elToWrite, currentOffset++);
    }


    public void flush() {

    }
}
