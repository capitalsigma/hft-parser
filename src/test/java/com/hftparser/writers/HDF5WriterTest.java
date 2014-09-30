package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5CompoundType;
import ch.systemsx.cisd.hdf5.IHDF5CompoundWriter;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.containers.WaitFreeQueue;
import com.hftparser.readers.DataPoint;
import com.hftparser.readers.WritableDataPoint;
import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

//import ncsa.hdf.object.FileFormat;


@RunWith(JMockit.class)
public class HDF5WriterTest {
    private HDF5Writer writer;
    private WaitFreeQueue<DataPoint> inQ;
    private final String OUT_FILE_PATH = "test-out.h5";
    private MutableBoolean pipelineError;

    public static class MockDSBridge extends MockUp<HDF5CompoundDSBridge<WritableDataPoint>> {

        @Mock
        public void $init(DatasetName name,
                         HDF5CompoundType<WritableDataPoint> type,
                         IHDF5CompoundWriter writer,
                         long startSize,
                         int chunkSize,
                         HDF5CompoundDSBridgeConfig bridgeConfig) {
//          don't care, just need appendElement
            System.out.println("Calling init");
        }

        @Mock
        public void appendElement(WritableDataPoint point) throws HDF5CompoundDSBridge.FailedWriteError {
            System.out.println("About to throw exn for appendElement");
            throw new HDF5CompoundDSBridge.FailedWriteError(new Exception("Test"));

        }
    }


    @Before
    public void setUp() throws Exception {
        pipelineError = new MutableBoolean();
        inQ = new WaitFreeQueue<>(5);
        writer = new HDF5Writer(inQ, new File(OUT_FILE_PATH), pipelineError);
        writer.setCloseFileAtEnd(true);

    }

    @Test
    public void testRun() throws Exception {
        DataPoint testPoint1 = new DataPoint("FOO", new long[][]{{1, 2}}, new long[][]{{3, 4}}, 6, 10l);
        DataPoint testPoint2 = new DataPoint("FOO", new long[][]{{4, 5}}, new long[][]{{6, 7}}, 7, 101l);

        WritableDataPoint expected1 = testPoint1.getWritable();
        WritableDataPoint expected2 = testPoint2.getWritable();
        HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>> tickerMap = writer.getDsForTicker();

        inQ.enq(testPoint1);
        inQ.enq(testPoint2);

        Thread runThread = new Thread(writer);

        runThread.start();

        Thread.sleep(100);

        HDF5CompoundDSBridge<WritableDataPoint> dsBridge = tickerMap.get("FOO");

        assertTrue(expected1.equals(dsBridge.readBlock(0)[0]));
        assertTrue(expected2.equals(dsBridge.readBlock(1)[0]));
        assertFalse(pipelineError.booleanValue());
    }

    @Test
    public void testPipelineError() throws Exception {
        System.out.println("About to start test.");

        new MockDSBridge();

        System.out.println("Built mock bridge");

        DataPoint testPoint1 = new DataPoint("FOO", new long[][]{{1, 2}}, new long[][]{{3, 4}}, 6, 10l);
        DataPoint testPoint2 = new DataPoint("FOO", new long[][]{{4, 5}}, new long[][]{{6, 7}}, 7, 101l);

        WritableDataPoint expected1 = testPoint1.getWritable();
        WritableDataPoint expected2 = testPoint2.getWritable();
        HashMap<String, HDF5CompoundDSBridge<WritableDataPoint>> tickerMap = writer.getDsForTicker();

        inQ.enq(testPoint1);

        System.out.println("About to build writer thread.");

        Thread runThread = new Thread(writer);

        System.out.println("About to start thread");

        runThread.start();

        System.out.println("Started, about to sleep");

        Thread.sleep(100);

        System.out.println("Done sleeping");

        assertTrue(pipelineError.booleanValue());
    }

    @After
    public void tearDown() throws Exception {
        inQ.acceptingOrders = false;
        pipelineError.setValue(false);
        Thread.sleep(200);
    }
}
