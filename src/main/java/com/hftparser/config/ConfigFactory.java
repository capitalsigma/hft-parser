package com.hftparser.config;

import com.hftparser.config.*;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigFactory {
    private final ParseRunConfig parseRunConfig;
    private final ArcaParserConfig arcaParserConfig;
    private final HDF5WriterConfig hdf5WriterConfig;
    private final HDF5CompoundDSBridgeConfig hdf5CompoundDSBridgeConfig;
    private final MarketOrderCollectionConfig marketOrderCollectionConfig;

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

    public MarketOrderCollectionConfig getMarketOrderCollectionConfig() {
        return marketOrderCollectionConfig;
    }

    public static ConfigFactory fromPath(String path) throws BadConfigFileError, IOException {
        String jsonStr = new String(Files.readAllBytes(Paths.get(path)), "UTF8");
        return new ConfigFactory(jsonStr);
    }

    private ConfigFactory(String jsonStr) throws BadConfigFileError {
        JSONObject jsonObject = new JSONObject(jsonStr);

        parseRunConfig = new ParseRunConfig(jsonObject.getJSONObject("ParseRun"));
        arcaParserConfig = new ArcaParserConfig(jsonObject.getJSONObject("ArcaParser"));
        hdf5WriterConfig = new HDF5WriterConfig(jsonObject.getJSONObject("HDF5Writer"));
        hdf5CompoundDSBridgeConfig = new HDF5CompoundDSBridgeConfig(jsonObject.getJSONObject("HDF5CompoundDSBridge"));

        if (jsonObject.has("MarketOrderCollection")) {
            marketOrderCollectionConfig =
                    new MarketOrderCollectionConfig(jsonObject.getJSONObject("MarketOrderCollection"));
        } else {
            marketOrderCollectionConfig = null;
        }

    }
}
