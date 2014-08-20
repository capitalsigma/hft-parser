package com.hftparser.writers;

import com.hftparser.config.ConfigFactory;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class HDF5CompoundDSParallelFlushBridgeTest extends HDF5CompoundDSAsyncBridgeTest {
    protected String TEST_CONFIG = "src/test/resources/parallel-flush-config.json";

    @Override
    protected void setConfig() throws Exception {
        ConfigFactory factory = ConfigFactory.fromPath(TEST_CONFIG);
        config = factory.getHdf5CompoundDSBridgeConfig();

        System.out.println("Got config:" + config.toString());
    }
//
    @Override
    protected void buildBridge(HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder) throws Exception {
        System.out.println("Got builder: " + dtBuilder.toString());
        //        dtBuilder.setAsync(true);
        this.setDtBridge(dtBuilder.buildParallelFlush(TEST_DS));
        assertNotNull(getDtBridge());
    }

    @Override
    protected void testForWriter() {
        assertNotNull(getDtBridge().getLastWriter());
    }

//    @After
//    @Override
//    public void tearDown() throws Exception {
//        super.tearDown();
//
//        assertThat(dtBridge instanceof HDF5CompoundDSParallelFlushBridge, is(true));
//    }
}