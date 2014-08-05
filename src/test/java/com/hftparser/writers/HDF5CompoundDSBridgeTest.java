package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class HDF5CompoundDSBridgeTest {
    private final String TEST_PATH = "test-out.h5";
    private final DatasetName TEST_DS = new DatasetName("group", "foo");
    private WritableDataPoint testPoint1;
    private WritableDataPoint testPoint2;
    private IHDF5Writer writer;

    private HDF5CompoundDSBridge<WritableDataPoint> dtBridge;

    @Before
    public void setUp() throws  Exception {
        try {
            File file = new File(TEST_PATH);

            testPoint1 = new WritableDataPoint(new long[][]{{1, 2}}, new long[][] {{3, 4}}, 6, 10l);
            testPoint2 = new WritableDataPoint(new long[][]{{4, 5}}, new long[][] {{6, 7}}, 7, 101l);

            writer = HDF5Writer.getWriter(file);
            HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder = new HDF5CompoundDSBridgeBuilder<>(writer);
            dtBuilder.setChunkSize(5);
            dtBuilder.setStartSize(10);
            dtBuilder.setTypeFromInferred(WritableDataPoint.class);

            dtBridge =  dtBuilder.build(TEST_DS);

        } catch (StackOverflowError e) {
            fail("This library has a bug in HDF5GenericStorageFeatures.java line 425, " +
                    "that throws it into an infinite loop if .defaultStorageLayout is called. NEVER, " +
                    "EVER CALL defaultStorageLayout.\n Failed with error: " + e.toString());
        }
    }

    @Test
    public void testAppendElement() throws Exception {
        dtBridge.appendElement(testPoint1);
        dtBridge.appendElement(testPoint2);

        assertTrue(testPoint1.equals(dtBridge.readBlock(0)[0]));
        assertTrue(testPoint2.equals(dtBridge.readBlock(1)[0]));
    }

    @Test
    public void testDoesntOverflowStackBecauseOfALibraryBug() {
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
    }
}