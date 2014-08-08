package com.hftparser.writers;

import com.hftparser.containers.WaitFreeQueue;
import com.hftparser.readers.DataPoint;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

//import ncsa.hdf.object.FileFormat;

@RunWith(JUnit4.class)
public class HDF5WriterTest {
    private HDF5Writer writer;
    private WaitFreeQueue<DataPoint> inQ;
    private final String OUT_FILE_PATH = "test-out.h5";

    @Before
    public void setUp() throws Exception {
        inQ = new WaitFreeQueue<>(5);
        writer = new HDF5Writer(inQ, new File(OUT_FILE_PATH));
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

        //        inQ.acceptingOrders = false;
        //
        //        runThread.join();


        HDF5CompoundDSBridge<WritableDataPoint> dsBridge = tickerMap.get("FOO");

        assertTrue(expected1.equals(dsBridge.readBlock(0)[0]));
        assertTrue(expected2.equals(dsBridge.readBlock(1)[0]));
    }

    @After
    public void tearDown() throws Exception {
        inQ.acceptingOrders = false;
        Thread.sleep(20);
    }
}
