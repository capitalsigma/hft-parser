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

import static org.junit.Assert.*;

public class HDF5CompoundDSCutoffCachingBridgeTest {
    protected final String TEST_PATH = "test-out.h5";
    protected final DatasetName TEST_DS = new DatasetName("group", "foo");
    protected WritableDataPoint testPoint1;
    protected WritableDataPoint emptyPoint;
    protected WritableDataPoint[] emptyPoints;
    protected WritableDataPoint[] fullPoints;
    protected IHDF5Writer writer;
    protected HDF5CompoundDSBridgeConfig config;


    protected HDF5CompoundDSCachingBridge<WritableDataPoint> dtBridge;


    protected HDF5CompoundDSCachingBridge<WritableDataPoint> getDtBridge() {
        return dtBridge;
    }

    @Before
    public void setUp() throws Exception {
        try {
            System.out.println("Calling setUp");
            File file = new File(TEST_PATH);

            testPoint1 = new WritableDataPoint(new long[][]{{1, 2}}, new long[][]{{3, 4}}, 6, 10l);
            emptyPoint = new WritableDataPoint(new long[][]{}, new long[][]{}, 0, 0l);

            emptyPoints = new WritableDataPoint[]{emptyPoint, emptyPoint, emptyPoint, emptyPoint, emptyPoint,};
            fullPoints = new WritableDataPoint[]{testPoint1, testPoint1, testPoint1, testPoint1, testPoint1,};


            config = new HDF5CompoundDSBridgeConfig(HDF5StorageLayout.CHUNKED,
                                                    HDF5GenericStorageFeatures.MAX_DEFLATION_LEVEL,
                                                    5);

            writer = HDF5Writer.getWriter(file);
            HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder =
                    new HDF5CompoundDSBridgeBuilder<>(writer, config);
            dtBuilder.setChunkSize(5);
            dtBuilder.setStartSize(5);
            dtBuilder.setTypeFromInferred(WritableDataPoint.class);
            dtBuilder.setCutoff(true);

            buildBridge(dtBuilder);

        } catch (StackOverflowError e) {
            fail("This library has a bug in HDF5GenericStorageFeatures.java line 425, " +
                         "that throws it into an infinite loop if .defaultStorageLayout is called. NEVER, " +
                         "EVER CALL defaultStorageLayout.\n Failed with error: " + e.toString());
        }

    }

    protected void buildBridge(HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder) throws Exception {
        setDtBridge(dtBuilder.buildCaching(TEST_DS));
    }

    @After
    public void tearDown() throws Exception {
        writer.close();
    }

    @Test
    public void testAppendElement() throws Exception {
        for (int i = 0; i < 4; i++) {
            getDtBridge().appendElement(testPoint1);
            System.out.println("Got: " + Arrays.deepToString(getDtBridge().readBlock(0, 5)));
            assertTrue(Arrays.deepEquals(getDtBridge().readBlock(0, 5), emptyPoints));
        }
        getDtBridge().appendElement(testPoint1);
        //        System.out.println("Got: " + Arrays.deepToString(dtBridge.readBlock(0, 5)));
        assertTrue(Arrays.deepEquals(getDtBridge().readBlock(0, 5), fullPoints));
    }

    @Test
    public void testFlush() throws Exception {
        WritableDataPoint[] threeEmpty = new WritableDataPoint[]{emptyPoint, emptyPoint, emptyPoint};
        WritableDataPoint[] onePointBlock = new WritableDataPoint[]{testPoint1, emptyPoint, emptyPoint};

        assertTrue(Arrays.deepEquals(getDtBridge().readBlock(0, 3), threeEmpty));

        getDtBridge().appendElement(testPoint1);

        assertTrue(Arrays.deepEquals(getDtBridge().readBlock(0, 3), threeEmpty));

        getDtBridge().flush();

        assertTrue(Arrays.deepEquals(getDtBridge().readBlock(0, 3), onePointBlock));
    }

    @Test
    public void testCutoffExtraEqual() throws Exception {
        //        Note that we don't do this anymore, but it looks like readBlock just gives us zero-value elements
        // if the requested element doesn't exist. So we'll leave this test.
        WritableDataPoint[] expected = new WritableDataPoint[]{testPoint1, testPoint1, testPoint1, testPoint1,
                testPoint1, testPoint1};
        WritableDataPoint[] actual;

        for (int i = 0; i < 6; i++) {
            getDtBridge().appendElement(testPoint1);
        }
        getDtBridge().flush();

        actual = getDtBridge().readBlock(0, 10);

        System.out.println("Got: (testZeroOutExtra) " + Arrays.deepToString(actual));

        assertTrue(Arrays.deepEquals(expected, actual));
    }

    @Test
    public void testCuttofExtraCorrectLength() throws Exception {
        for (int i = 0; i < 6; i++) {
            getDtBridge().appendElement(testPoint1);
        }

        getDtBridge().flush();

        assertEquals(6, getDtBridge().readArray().length);
    }

    protected void setDtBridge(HDF5CompoundDSCachingBridge<WritableDataPoint> dtBridge) {
        this.dtBridge = dtBridge;
    }
}