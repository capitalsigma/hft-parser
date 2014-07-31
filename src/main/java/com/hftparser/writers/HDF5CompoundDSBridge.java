package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;

/**
 * Created by patrick on 7/28/14.
 */
class HDF5CompoundDSBridge<T> {
        private static HDF5GenericStorageFeatures features;
        private long currentOffset;
        private IHDF5CompoundWriter writer;
        private String fullPath;
        private T[] elToWrite;
        private HDF5CompoundType<T> type;

        public HDF5CompoundDSBridge(DatasetName name, HDF5CompoundType<T> type, IHDF5CompoundWriter writer,
                                    long startSize, int chunkSize) {
            fullPath = name.getFullPath();
            this.writer = writer;
            this.type = type;

            if(features == null){
                initFeatures();
            }

            this.writer.createArray(fullPath, type, startSize, chunkSize);
            currentOffset = 0;
            //noinspection unchecked
            elToWrite = (T[]) new Object[1];
        }

        private void initFeatures() {
            HDF5GenericStorageFeatures.HDF5GenericStorageFeatureBuilder featureBuilder=
                    HDF5GenericStorageFeatures.build();
//            featureBuilder.contiguousStorageLayout();
            featureBuilder.chunkedStorageLayout();
            featureBuilder.datasetReplacementEnforceKeepExisting();
//            featureBuilder.compress();

            features = featureBuilder.features();
        }

        public void appendElement(T element) {
            elToWrite[0] = element;
            writer.writeArrayBlockWithOffset(fullPath, type, elToWrite, currentOffset++);
        }

        T[] readBlock(long offset, int blocksize) {
            return writer.readArrayBlockWithOffset(fullPath, type, blocksize, offset);
        }

        public T[] readBlock(long offset){
            return readBlock(offset, 1);
        }
}
