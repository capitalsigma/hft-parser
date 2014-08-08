package com.hftparser.writers;

import com.hftparser.config.ConfigFactory;
import com.hftparser.readers.WritableDataPoint;
import org.hamcrest.CoreMatchers;
import org.junit.Before;

import static org.junit.Assert.*;

public class HDF5CompoundDSAsyncBridgeTest extends HDF5CompoundDSCutoffCachingBridgeTest {
    protected HDF5CompoundDSAsyncBridge<WritableDataPoint> dtBridge;
    String TEST_CONFIG = "src/test/resources/async-config.json";


    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

    }

    @Override
    protected void setConfig() throws Exception {
        ConfigFactory factory = ConfigFactory.fromPath(TEST_CONFIG);
        config = factory.getHdf5CompoundDSBridgeConfig();

        System.out.println("Got config:" + config.toString());
    }

    @Override
    protected void buildBridge(HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder) throws Exception {
        System.out.println("Got builder: " + dtBuilder.toString());
        //        dtBuilder.setAsync(true);
        this.setDtBridge(dtBuilder.buildAsync(TEST_DS));
        assertNotNull(getDtBridge());
    }

    public void testWriterWasSpawned() throws Exception {
        HDF5CompoundDSAsyncBridge.Writer writer = getDtBridge().getLastWriter();
        Thread.sleep(200);

        assertNotNull(writer);
        assertThat(writer.isDone(), CoreMatchers.is(true));
    }

    @Override
    public void testAppendElement() throws Exception {
        super.testAppendElement();
        testWriterWasSpawned();
    }

    @Override
    public void testFlush() throws Exception {
        super.testFlush();
        assertNull(getDtBridge().getLastWriter());
    }

    @Override
    public void testCutoffExtraEqual() throws Exception {
        super.testCutoffExtraEqual();

        testWriterWasSpawned();
    }

    @Override
    public void testCuttofExtraCorrectLength() throws Exception {
        super.testCuttofExtraCorrectLength();
        testWriterWasSpawned();
    }

    @Override
    protected HDF5CompoundDSAsyncBridge<WritableDataPoint> getDtBridge() {
        return dtBridge;
    }

    protected void setDtBridge(HDF5CompoundDSAsyncBridge<WritableDataPoint> dtBridge) {
        this.dtBridge = dtBridge;
    }
}