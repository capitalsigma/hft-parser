package com.hftparser.main;

import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import org.json.JSONObject;

/**
 * Created by patrick on 7/31/14.
 */

class BadConfigFileError extends Exception{

}

class ParseRunConfig {
    private final int line_queue_size;
    private final int point_queue_size;
    private final boolean backoff;
    private final int min_backoff_s;
    private final int max_backoff_s;
    private final int min_backoff_d;

    public int getLine_queue_size() {
        return line_queue_size;
    }

    public int getPoint_queue_size() {
        return point_queue_size;
    }

    public boolean isBackoff() {
        return backoff;
    }

    public int getMin_backoff_s() {
        return min_backoff_s;
    }

    public int getMax_backoff_s() {
        return max_backoff_s;
    }

    public int getMin_backoff_d() {
        return min_backoff_d;
    }

    public int getMax_backoff_d() {
        return max_backoff_d;
    }

    private final int max_backoff_d;

    ParseRunConfig(JSONObject json) {
        line_queue_size = json.getInt("line_queue_size");
        point_queue_size = json.getInt("point_queue_size");
        backoff = json.getBoolean("backoff");
        min_backoff_s = json.getInt("min_backoff_s");
        max_backoff_s = json.getInt("max_backoff_s");
        min_backoff_d = json.getInt("min_backoff_d");
        max_backoff_d = json.getInt("max_backoff_d");
    }
}

class ArcaParserConfig {
    private final int initial_order_history_size;
    private final int output_progress_every;

    public int getInitial_order_history_size() {
        return initial_order_history_size;
    }

    public int getOutput_progress_every() {
        return output_progress_every;
    }

    ArcaParserConfig(JSONObject json) {
        initial_order_history_size = json.getInt("initial_order_history_size");
        output_progress_every = json.getInt("output_progress_every");
    }
}

class HDF5WriterConfig {
    private final int start_size;
    private final int chunk_size;
    private final IHDF5WriterConfigurator.SyncMode sync_mode;
    private final boolean overwrite;
    private boolean keep_datasets_if_they_exist;
    private boolean perform_numeric_conversions;


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
}

class HDF5CompoundDSBridgeConfig {
    private final boolean default_storage_layout;
    private final HDF5StorageLayout storage_layout;
    private final byte deflate_level;

    HDF5CompoundDSBridgeConfig(JSONObject json) throws BadConfigFileError {
        default_storage_layout = json.getBoolean("default_storage_layout ");

        switch (json.getString("storage_layout")) {
            case "COMPACT":
                storage_layout = HDF5StorageLayout.COMPACT;
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

    public boolean isDefault_storage_layout() {
        return default_storage_layout;
    }

    public HDF5StorageLayout getStorage_layout() {
        return storage_layout;
    }

    public byte getDeflate_level() {
        return deflate_level;
    }
}

class ConfigFactory {
    private final ParseRunConfig parseRunConfig;
    private final ArcaParserConfig arcaParserConfig;
    private final HDF5WriterConfig hdf5WriterConfig;
    private final HDF5CompoundDSBridgeConfig hdf5CompoundDSBridgeConfig;

    public HDF5CompoundDSBridgeConfig getHdf5CompoundDSBridgeConfig() {
        return hdf5CompoundDSBridgeConfig;
    }

    public ParseRunConfig getParseRunConfig() {
        return parseRunConfig;
    }

    public ArcaParserConfig getArcaParserConfig() {
        return arcaParserConfig;
    }

    public HDF5WriterConfig getHdf5WriterConfig() {
        return hdf5WriterConfig;
    }


    public ConfigFactory(String jsonStr) throws BadConfigFileError {
        JSONObject jsonObject = new JSONObject(jsonStr);

        parseRunConfig = new ParseRunConfig(jsonObject.getJSONObject("ParseRun"));
        arcaParserConfig = new ArcaParserConfig(jsonObject.getJSONObject("ArcaParser"));
        hdf5WriterConfig = new HDF5WriterConfig(jsonObject.getJSONObject("HDF5Writer"));
        hdf5CompoundDSBridgeConfig = new HDF5CompoundDSBridgeConfig(jsonObject.getJSONObject("HDF5CompoundDSBridge"));
    }
}
