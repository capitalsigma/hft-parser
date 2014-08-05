//package com.hftparser.writers;
//
//import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
//import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
//import ch.systemsx.cisd.hdf5.IHDF5Writer;
//import com.hftparser.config.HDF5CompoundDSBridgeConfig;
//import com.hftparser.readers.PythonWritableDataPoint;
//import com.hftparser.readers.WritableDataPoint;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.File;
//import java.util.Arrays;
//
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//
///**
// * Created by patrick on 8/4/14.
// */
//public class WritePythonDataPointTest {
//    private final String TEST_PATH = "test-out.h5";
//    private final DatasetName TEST_DS = new DatasetName("group", "foo");
//    private PythonWritableDataPoint testPoint1;
//    private PythonWritableDataPoint emptyPoint;
//    private PythonWritableDataPoint[] emptyPoints;
//    private PythonWritableDataPoint[] fullPoints;
//    private IHDF5Writer writer;
//
//    private HDF5CompoundDSBridge<PythonWritableDataPoint> dtBridge;
//
//    @Before
//    public void setUp() throws Exception {
//        try {
//            File file = new File(TEST_PATH);
//
//
//
//            testPoint1 = new PythonWritableDataPoint(new int[][]{{1, 2}}, new int[][]{{3, 4}}, 6l, "None",  10l);
//            emptyPoint = new PythonWritableDataPoint(new int[][]{}, new int[][]{}, 0, "", 0l);
//
//            emptyPoints = new PythonWritableDataPoint[]{emptyPoint, emptyPoint, emptyPoint, emptyPoint, emptyPoint,};
//            fullPoints = new PythonWritableDataPoint[]{testPoint1, testPoint1, testPoint1, testPoint1, testPoint1,};
//
//
//            HDF5CompoundDSBridgeConfig config = new HDF5CompoundDSBridgeConfig(HDF5StorageLayout.CHUNKED,
//                                                                               HDF5GenericStorageFeatures
//                                                                                       .MAX_DEFLATION_LEVEL, 5);
//
//            writer = HDF5Writer.getWriter(file);
//            HDF5CompoundDSBridgeBuilder<PythonWritableDataPoint> dtBuilder = new HDF5CompoundDSBridgeBuilder<>(writer, config);
//            dtBuilder.setChunkSize(5);
//            dtBuilder.setStartSize(5);
//            dtBuilder.setTypeFromInferred(PythonWritableDataPoint.class);
//            dtBuilder.setCutoff(false);
//
//            dtBridge = dtBuilder.build(TEST_DS);
//
//        } catch (StackOverflowError e) {
//            fail("This library has a bug in HDF5GenericStorageFeatures.java line 425, " +
//                         "that throws it into an infinite loop if .defaultStorageLayout is called. NEVER, " +
//                         "EVER CALL defaultStorageLayout.\n Failed with error: " + e.toString());
//        }
//
//    }
//
//    @Test
//    public void testAppendElement() throws Exception {
//        for (int i = 0; i < 5; i++) {
//            dtBridge.appendElement(testPoint1);
//            System.out.println("Got: " + Arrays.deepToString(dtBridge.readBlock(0, 5)));
////            assertTrue(Arrays.deepEquals(dtBridge.readBlock(0, 5), emptyPoints));
//        }
//
//        dtBridge.flush();
////        System.out.println("Got: " + Arrays.deepToString(dtBridge.readBlock(0, 5)));
//        assertTrue(Arrays.deepEquals(dtBridge.readBlock(0, 5), fullPoints));
//    }
//}
