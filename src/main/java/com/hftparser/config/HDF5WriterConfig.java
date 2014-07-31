package com.hftparser.config;

import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import org.json.JSONObject;

/**
 * Created by patrick on 7/31/14.
 */
public class HDF5WriterConfig {
    private final int start_size;
    private final int chunk_size;
    private final IHDF5WriterConfigurator.SyncMode sync_mode;
    private final boolean overwrite;
    private final boolean keep_datasets_if_they_exist;
    private final boolean perform_numeric_conversions;


    public int getStart_size() {
        return start_size;
    }

    public int getChunk_size() {
        return chunk_size;
    }

    public IHDF5WriterConfigurator.SyncMode getSync_mode() {
        return sync_mode;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public boolean isKeep_datasets_if_they_exist() {
        return keep_datasets_if_they_exist;
    }

    public boolean isPerform_numeric_conversions() {
        return perform_numeric_conversions;
    }


    HDF5WriterConfig(JSONObject json) throws BadConfigFileError {
        start_size = json.getInt("start_size");
        chunk_size = json.getInt("chunk_size");

        switch(json.getString("sync_mode")) {
        case "SYNC":
            sync_mode = IHDF5WriterConfigurator.SyncMode.SYNC;
            break;
        default:
                throw new BadConfigFileError();
        }

        overwrite = json.getBoolean("overwrite");
        keep_datasets_if_they_exist = json.getBoolean("keep_datasets_if_they_exist");
        perform_numeric_conversions = json.getBoolean("perform_numeric_conversions");
    }

    public HDF5WriterConfig(boolean perform_numeric_conversions,
                            boolean keep_datasets_if_they_exist,
                            boolean overwrite,
                            IHDF5WriterConfigurator.SyncMode sync_mode,
                            int chunk_size,
                            int start_size) {
        this.perform_numeric_conversions = perform_numeric_conversions;
        this.keep_datasets_if_they_exist = keep_datasets_if_they_exist;
        this.overwrite = overwrite;
        this.sync_mode = sync_mode;
        this.chunk_size = chunk_size;
        this.start_size = start_size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HDF5WriterConfig that = (HDF5WriterConfig) o;

        if (chunk_size != that.chunk_size) return false;
        if (keep_datasets_if_they_exist != that.keep_datasets_if_they_exist) return false;
        if (overwrite != that.overwrite) return false;
        if (perform_numeric_conversions != that.perform_numeric_conversions) return false;
        if (start_size != that.start_size) return false;
        if (sync_mode != that.sync_mode) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = start_size;
        result = 31 * result + chunk_size;
        result = 31 * result + sync_mode.hashCode();
        result = 31 * result + (overwrite ? 1 : 0);
        result = 31 * result + (keep_datasets_if_they_exist ? 1 : 0);
        result = 31 * result + (perform_numeric_conversions ? 1 : 0);
        return result;
    }

    public static HDF5WriterConfig getDefault() {
        return new HDF5WriterConfig(true, true, true, IHDF5WriterConfigurator.SyncMode
                .SYNC_BLOCK, 100, 10);
    }
}
