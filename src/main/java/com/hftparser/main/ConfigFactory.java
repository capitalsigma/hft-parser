package com.hftparser.main;

import org.json.JSONObject;

/**
 * Created by patrick on 7/31/14.
 */
class ParseRunConfig {
    ParseRunConfig(JSONObject json) {

    }
}

class ArcaParserConfig {
    ArcaParserConfig(JSONObject json) {
    }
}

class HDF5WriterConfig {
    HDF5WriterConfig(JSONObject json) {
    }
}

class HDF5CompoundDSBridgeConfig {
    HDF5CompoundDSBridgeConfig(JSONObject json) {
    }
}

class ConfigFactory {
    private final ParseRunConfig parseRunConfig;
    private final ArcaParserConfig arcaParserConfig;
    private final HDF5WriterConfig hdf5WriterConfig;
    private final HDF5CompoundDSBridgeConfig hdf5CompoundDSBridgeConfig;

    public ConfigFactory(String jsonStr) {
        JSONObject jsonObject = new JSONObject(jsonStr);

        parseRunConfig = new ParseRunConfig(jsonObject.getJSONObject("ParseRun"));
        arcaParserConfig = new ArcaParserConfig(jsonObject.getJSONObject("ArcaParser"));
        hdf5WriterConfig = new HDF5WriterConfig(jsonObject.getJSONObject("HDF5Writer"));
        hdf5CompoundDSBridgeConfig = new HDF5CompoundDSBridgeConfig(jsonObject.getJSONObject("HDF5CompoundDSBridge"));

    }
}
