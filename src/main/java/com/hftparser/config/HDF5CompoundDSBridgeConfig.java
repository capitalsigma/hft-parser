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
    private int cache_size;
    private boolean cutoff;

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

        String fieldVal = json.optString("deflate_level", "NOSTR");
//        System.out.println("Got: " + fieldVal);
        switch (json.optString("deflate_level", "NOSTR")) {
            case "MAX_DEFLATION_LEVEL":
                deflate_level = HDF5GenericStorageFeatures.MAX_DEFLATION_LEVEL;
                break;
            case "DEFAULT_DEFLATION_LEVEL":
                deflate_level = HDF5GenericStorageFeatures.DEFAULT_DEFLATION_LEVEL;
                break;
            case "NO_DEFLATION_LEVEL":
                deflate_level = HDF5GenericStorageFeatures.NO_DEFLATION_LEVEL;
                break;
            case "NOSTR":
                throw new BadConfigFileError();
            default:
                deflate_level = (byte) json.getInt("deflate_level");
                break;
        }

        if (!json.has("cache_size")) {
            cache_size = -1;
        }  else {
            cache_size = json.getInt("cache_size");
        }

        if (!json.has("cutoff")) {
            cutoff = false;
        } else {
            cutoff = json.getBoolean("cutoff");
        }
   }

    public HDF5CompoundDSBridgeConfig(HDF5StorageLayout storage_layout, byte deflate_level) {
        //        this.default_storage_layout = default_storage_layout;
        this(storage_layout, deflate_level, -1);
    }

    public HDF5CompoundDSBridgeConfig(HDF5StorageLayout storage_layout, byte deflate_level,  int cache_size) {
        this.cache_size = cache_size;
        this.deflate_level = deflate_level;
        this.storage_layout = storage_layout;
        cutoff = false;
    }

    public HDF5CompoundDSBridgeConfig(HDF5StorageLayout storage_layout, byte deflate_level, int cache_size,
                                      boolean cutoff) {
        this.storage_layout = storage_layout;
        this.deflate_level = deflate_level;
        this.cache_size = cache_size;
        this.cutoff = cutoff;
    }

    public HDF5StorageLayout getStorage_layout() {
        return storage_layout;
    }

    public byte getDeflate_level() {
        return deflate_level;
    }

    public int getCache_size() {
        return cache_size;
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

    public boolean isCutoff() {
        return cutoff;
    }

    @Override
    public int hashCode() {
        int result = storage_layout.hashCode();
        result = 31 * result + (int) deflate_level;
        return result;
    }

    public static HDF5CompoundDSBridgeConfig getDefault() {
        return new HDF5CompoundDSBridgeConfig(HDF5StorageLayout.CHUNKED,
                HDF5GenericStorageFeatures.MAX_DEFLATION_LEVEL);
    }

    @Override
    public String toString() {
        return "HDF5CompoundDSBridgeConfig{" +
                "storage_layout=" + storage_layout +
                ", deflate_level=" + deflate_level +
                '}';
    }
}
