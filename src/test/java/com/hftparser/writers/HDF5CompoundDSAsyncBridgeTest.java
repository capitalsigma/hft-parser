package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.HDF5GenericStorageFeatures;
import ch.systemsx.cisd.hdf5.HDF5StorageLayout;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.readers.WritableDataPoint;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class HDF5CompoundDSAsyncBridgeTest extends HDF5CompoundDSCutoffCachingBridgeTest {
    protected HDF5CompoundDSAsyncBridge<WritableDataPoint> dtBridge;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        System.out.println("Executing this setup.");

        Executor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(5));

        dtBridge = new HDF5CompoundDSAsyncBridge<>(super.dtBridge, executor);
    }

    public void testWriterWasSpawned() {
        HDF5CompoundDSAsyncBridge.Writer writer = dtBridge.getLastWriter();
        assertNotNull(writer);
        assertThat(writer.isDone(), CoreMatchers.is(true));
    }

    @Override
    public void testAppendElement() throws Exception {
        for (int i = 0; i < 4; i++) {
            dtBridge.appendElement(testPoint1);
            System.out.println("Got: " + Arrays.deepToString(this.dtBridge.readBlock(0, 5)));
            assertTrue(Arrays.deepEquals(this.dtBridge.readBlock(0, 5), emptyPoints));
        }
        this.dtBridge.appendElement(testPoint1);
        //        System.out.println("Got: " + Arrays.deepToString(dtBridge.readBlock(0, 5)));
        assertTrue(Arrays.deepEquals(dtBridge.readBlock(0, 5), fullPoints));
//        super.testAppendElement();
        testWriterWasSpawned();
    }

    @Override
    public void testFlush() throws Exception {
        super.testFlush();
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
}