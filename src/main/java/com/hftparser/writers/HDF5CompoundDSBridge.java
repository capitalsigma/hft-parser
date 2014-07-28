package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5Writer;

/**
 * Created by patrick on 7/28/14.
 */
class HDF5CompoundDSBridge<T> {
        private static HDF5GenericStorageFeatures features;
        private long currentOffset;
        private IHDF5Writer writer;

        public HDF5CompoundDSBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5Writer writer,
                                    long startSize, int chunkSize) {
            String fullPath = name.getFullPath();
            this.writer = writer;

            if(features == null){
                initFeatures();
            }

            writer.compound().createArray(fullPath, type, startSize, chunkSize);
            currentOffset = 0;
        }

        private void initFeatures() {
            HDF5GenericStorageFeatures.HDF5GenericStorageFeatureBuilder featureBuilder=
                    HDF5GenericStorageFeatures.build();
            featureBuilder.chunkedStorageLayout();
            featureBuilder.datasetReplacementEnforceKeepExisting();

            features = featureBuilder.features();
        }

        private void appendElement(T element){
            writer.

        }
}
