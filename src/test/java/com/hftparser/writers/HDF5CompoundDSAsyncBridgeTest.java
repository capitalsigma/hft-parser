package com.hftparser.writers;

import com.hftparser.readers.WritableDataPoint;
import org.hamcrest.CoreMatchers;

import java.util.Arrays;

import static org.junit.Assert.*;

public class HDF5CompoundDSAsyncBridgeTest extends HDF5CompoundDSCutoffCachingBridgeTest {
    protected HDF5CompoundDSAsyncBridge<WritableDataPoint> dtBridge;



    @Override
    protected void buildBridge(HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder) throws Exception {
        dtBuilder.setAsync(true);
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
        for (int i = 0; i < 4; i++) {
            getDtBridge().appendElement(testPoint1);
            System.out.println("Got: " + Arrays.deepToString(this.getDtBridge().readBlock(0, 5)));
            assertTrue(Arrays.deepEquals(this.getDtBridge().readBlock(0, 5), emptyPoints));
        }
        this.getDtBridge().appendElement(testPoint1);
        //        System.out.println("Got: " + Arrays.deepToString(dtBridge.readBlock(0, 5)));
        assertTrue(Arrays.deepEquals(getDtBridge().readBlock(0, 5), fullPoints));
//        super.testAppendElement();
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