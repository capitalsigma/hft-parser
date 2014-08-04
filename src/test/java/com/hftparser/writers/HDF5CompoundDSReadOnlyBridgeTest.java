package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.HDF5DataBlock;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.HDF5WriterConfig;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class HDF5CompoundDSReadOnlyBridgeTest {
    private final String TEST_PATH = "test-out.h5";
    private final DatasetName TEST_DS = new DatasetName("group", "foo");
    private HDF5CompoundDSReadOnlyBridge<WritableDataPoint> dtBridge;
    private IHDF5Writer writer;
    private WritableDataPoint testPoint1;
    private WritableDataPoint testPoint2;


    @Before
    public void setUp() throws  Exception {
        try {
            File file = new File(TEST_PATH);

            testPoint1 = new WritableDataPoint(new int[][]{{1, 2}}, new int[][] {{3, 4}}, 6, 10l);
            testPoint2 = new WritableDataPoint(new int[][]{{4, 5}}, new int[][] {{6, 7}}, 7, 101l);

            writer = HDF5Writer.getWriter(file);

            HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder = new HDF5CompoundDSBridgeBuilder<>(writer);
            dtBuilder.setTypeFromInferred(WritableDataPoint.class);

            dtBridge = dtBuilder.buildReadOnly(TEST_DS);

            HDF5CompoundDSBridge<WritableDataPoint> dtWriter = dtBuilder.build(TEST_DS);

            dtWriter.appendElement(testPoint1);
            dtWriter.appendElement(testPoint2);

        } catch (StackOverflowError e) {
            fail("This library has a bug in HDF5GenericStorageFeatures.java line 425, " +
                    "that throws it into an infinite loop if .defaultStorageLayout is called. NEVER, " +
                    "EVER CALL defaultStorageLayout.\n Failed with error: " + e.toString());
        }
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
    }

    @Test
    public void testReadArray() throws Exception {
        WritableDataPoint[] blocks = dtBridge.readArray();

        assertEquals(testPoint1, blocks[0]);
        assertEquals(testPoint2, blocks[1]);
        assertEquals(blocks.length, 2);
    }
}