package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HDF5CompoundDSZeroOutCachingBridgeTest {
    private final String TEST_PATH = "test-out.h5";
    private final DatasetName TEST_DS = new DatasetName("group", "foo");
    private WritableDataPoint testPoint1;
    private WritableDataPoint emptyPoint;
    private WritableDataPoint[] emptyPoints;
    private WritableDataPoint[] fullPoints;
    private IHDF5Writer writer;

    private HDF5CompoundDSBridge<WritableDataPoint> dtBridge;

    @Before
    public void setUp() throws Exception {
        try {
            File file = new File(TEST_PATH);

            testPoint1 = new WritableDataPoint(new long[][]{{1, 2}}, new long[][]{{3, 4}}, 6, 10l);
            emptyPoint = new WritableDataPoint(new long[][]{}, new long[][]{}, 0, 0l);

            emptyPoints = new WritableDataPoint[]{emptyPoint, emptyPoint, emptyPoint, emptyPoint, emptyPoint,};
            fullPoints = new WritableDataPoint[]{testPoint1, testPoint1, testPoint1, testPoint1, testPoint1,};


            HDF5CompoundDSBridgeConfig config = new HDF5CompoundDSBridgeConfig(HDF5StorageLayout.CHUNKED,
                                                                               HDF5GenericStorageFeatures
                                                                                       .MAX_DEFLATION_LEVEL,
                                                                               5);

            writer = HDF5Writer.getWriter(file);
            HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder =
                    new HDF5CompoundDSBridgeBuilder<>(writer, config);
            dtBuilder.setChunkSize(5);
            dtBuilder.setStartSize(5);
            dtBuilder.setTypeFromInferred(WritableDataPoint.class);
            dtBuilder.setCutoff(false);

            dtBridge = dtBuilder.build(TEST_DS);

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
    public void testZeroOut() throws Exception {
        //        Note that we don't do this anymore, but it looks like readBlock just gives us zero-value elements
        // if the requested element doesn't exist. So we'll leave this test.
        WritableDataPoint[] expected =
                new WritableDataPoint[]{testPoint1, testPoint1, testPoint1, testPoint1, testPoint1, testPoint1,
                        emptyPoint, emptyPoint, emptyPoint, emptyPoint};
        WritableDataPoint[] actual;

        for (int i = 0; i < 6; i++) {
            dtBridge.appendElement(testPoint1);
        }
        dtBridge.flush();

        actual = dtBridge.readBlock(0, 10);

        System.out.println("Got: (testZeroOutExtra) " + Arrays.deepToString(actual));

        assertTrue(Arrays.deepEquals(expected, actual));
    }

}