package com.hftparser.config;

import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
import ch.systemsx.cisd.hdf5.IHDF5WriterConfigurator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

// TODO: we really want to catch all these JSONExceptions and rethrow a BadConfigFileError
public class ConfigFactoryTest {
    private final String TEST_INFILE = "src/test/resources/test-config.json";
    private final String TEST_NUMERIC = "src/test/resources/test-config-numeric-deflate.json";
    private ConfigFactory factory;

    @Before
    public void setUp() throws Exception {
        factory = ConfigFactory.fromPath(TEST_INFILE);
    }

    @Test
    public void testGetHdf5CompoundDSBridgeConfig() throws Exception {
        HDF5CompoundDSBridgeConfig expected = new HDF5CompoundDSBridgeConfig(HDF5StorageLayout.COMPACT,
                                                                             HDF5GenericStorageFeatures
                                                                                     .MAX_DEFLATION_LEVEL);
        HDF5CompoundDSBridgeConfig actual = factory.getHdf5CompoundDSBridgeConfig();

        assertEquals(expected, actual);
    }

    @Test
    public void testGetParseRunConfig() throws Exception {
        ParseRunConfig expected = new ParseRunConfig(500000, 500000, true, 20, 500, 20, 500);
        ParseRunConfig actual = factory.getParseRunConfig();

        assertEquals(expected, actual);
    }

    @Test
    public void testGetArcaParserConfig() throws Exception {
        ArcaParserConfig expected = new ArcaParserConfig(500000, 5000000);

        ArcaParserConfig actual = factory.getArcaParserConfig();

        System.out.println("Expected: " + expected.toString());
        System.out.println("Actual: " + actual.toString());

        assertEquals(expected, actual);
    }

    @Test
    public void testGetHdf5WriterConfig() throws Exception {
        HDF5WriterConfig expected =
                new HDF5WriterConfig(false, true, true, IHDF5WriterConfigurator.SyncMode.SYNC, 500000, 100);
        HDF5WriterConfig actual = factory.getHdf5WriterConfig();

        System.out.println("Expected: " + expected.toString());
        System.out.println("Actual: " + actual.toString());


        assertEquals(expected, actual);
    }

    @Test
    public void testGetHdf5CompoundDsBridgeConfigForNumeric() throws Exception {
        ConfigFactory numFactory = ConfigFactory.fromPath(TEST_NUMERIC);

        HDF5CompoundDSBridgeConfig expected = new HDF5CompoundDSBridgeConfig(HDF5StorageLayout.COMPACT, (byte) 2);
        HDF5CompoundDSBridgeConfig actual = numFactory.getHdf5CompoundDSBridgeConfig();

        System.out.println("Expected: " + expected.toString());
        System.out.println("Actual: " + actual.toString());

        assertEquals(expected, actual);
    }

    @Test
    public void testGetMarketOrderCollectionConfig() throws Exception {
        MarketOrderCollectionConfig expected = new MarketOrderCollectionConfig(100, 10, true);
        MarketOrderCollectionConfig actual = factory.getMarketOrderCollectionConfig();

        assertEquals(expected, actual);

    }
}