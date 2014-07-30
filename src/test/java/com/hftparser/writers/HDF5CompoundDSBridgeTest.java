package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.readers.DataPoint;
import com.hftparser.readers.OrderType;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class HDF5CompoundDSBridgeTest {
    private String TEST_PATH = "test-out.h5";
    private DatasetName TEST_DS = new DatasetName("group", "foo");
    private WritableDataPoint testPoint1;
    private WritableDataPoint testPoint2;
    private IHDF5Writer writer;

    HDF5CompoundDSBridge<WritableDataPoint> dtBridge;

    @Before
    public void setUp() throws  Exception {
        File file = new File(TEST_PATH);

        testPoint1 = new WritableDataPoint(new int[][]{{1, 2}}, new int[][] {{3, 4}}, 6, 10l);
        testPoint2 = new WritableDataPoint(new int[][]{{4, 5}}, new int[][] {{6, 7}}, 7, 101l);

        writer = HDF5Writer.getDefaultWriter(file);
        HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder = new HDF5CompoundDSBridgeBuilder<WritableDataPoint>(writer);
        dtBuilder.setChunkSize(5);
        dtBuilder.setStartSize(10);
        dtBuilder.setTypeFromInferred(WritableDataPoint.class);

        dtBridge =  dtBuilder.build(TEST_DS);
    }

    @Test
    public void testAppendElement() throws Exception {
        dtBridge.appendElement(testPoint1);
        dtBridge.appendElement(testPoint2);

        assertTrue(testPoint1.equals(dtBridge.readBlock(0)[0]));
        assertTrue(testPoint2.equals(dtBridge.readBlock(1)[0]));
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
    }
}