package com.hftparser.config;

import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
import org.json.JSONObject;

/**
 * Created by patrick on 7/31/14.
 */
public class HDF5CompoundDSBridgeConfig {
    private final HDF5StorageLayout storage_layout;
    private final byte deflate_level;

    HDF5CompoundDSBridgeConfig(JSONObject json) throws BadConfigFileError {


        switch (json.getString("storage_layout")) {
            case "COMPACT":
                storage_layout = HDF5StorageLayout.COMPACT;
                break;
            case "CHUNKED":
                storage_layout = HDF5StorageLayout.CHUNKED;
                break;
            case "CONTIGUOUS":
                storage_layout = HDF5StorageLayout.CONTIGUOUS;
                break;
            default:
                throw new BadConfigFileError();
        }

        switch (json.getString("deflate_level")) {
            case "MAX_DEFLATION_LEVEL":
                deflate_level = HDF5GenericStorageFeatures.MAX_DEFLATION_LEVEL;
                break;
            case "DEFAULT_DEFLATION_LEVEL":
                deflate_level = HDF5GenericStorageFeatures.DEFAULT_DEFLATION_LEVEL;
                break;
            case "NO_DEFLATION_LEVEL":
                deflate_level = HDF5GenericStorageFeatures.NO_DEFLATION_LEVEL;
                break;
            default:
                throw new BadConfigFileError();
        }
    }

    public HDF5CompoundDSBridgeConfig(HDF5StorageLayout storage_layout, byte deflate_level) {
//        this.default_storage_layout = default_storage_layout;
        this.storage_layout = storage_layout;
        this.deflate_level = deflate_level;
    }

    public HDF5StorageLayout getStorage_layout() {
        return storage_layout;
    }

    public byte getDeflate_level() {
        return deflate_level;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HDF5CompoundDSBridgeConfig that = (HDF5CompoundDSBridgeConfig) o;

        if (deflate_level != that.deflate_level) {
            return false;
        }
        if (storage_layout != that.storage_layout) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = storage_layout.hashCode();
        result = 31 * result + (int) deflate_level;
        return result;
    }

    public static HDF5CompoundDSBridgeConfig getDefault() {
        return new HDF5CompoundDSBridgeConfig(HDF5StorageLayout.COMPACT,
                HDF5GenericStorageFeatures.MAX_DEFLATION_LEVEL);
    }
}
