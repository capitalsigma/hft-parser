package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.readers.WritableDataPoint;
import com.hftparser.writers.HDF5CompoundDSBridgeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HDF5CompoundVerifierTest {
    String TEST_DS_PATH = "src/test/resources/aapl-first-201.h5";
    String TEST_DS_MISSING_LINE_PATH = "src/test/resources/aapl-first-200.h5";
    HDF5CompoundVerifier<WritableDataPoint> verifier;


    @Before
    public void setUp() throws Exception {


    }

    @After
    public void tearDown() throws Exception {
        verifier.closeReaders();
    }

    private void setVerifierForSame() throws Exception {
        setVerifierForPaths(TEST_DS_PATH, TEST_DS_PATH);

    }

    private void setVerifierForPaths(String actualPath, String expectedPath) throws Exception {
        IHDF5Writer expectedWriter = HDF5Factory.open(TEST_DS_PATH);
        IHDF5Writer actualWriter = HDF5Factory.open(TEST_DS_PATH);

        verifier = new HDF5CompoundVerifier<>(actualWriter, expectedWriter, WritableDataPoint.class);

        assertNotNull(verifier);

        System.out.println("Set verifier: " + verifier);
    }

    @Test
    public void testCompareSame() throws Exception {
        setVerifierForSame();

        assertTrue("Identical datasets should be equal", verifier.compare());
    }

    @Test
    public void testDiffSame() throws Exception {
        setVerifierForSame();

        assertNull("Diff object must be null", verifier.diff());
    }

    DiffElement testDiffDSMissing(String expected, String actual) throws Exception {
        setVerifierForPaths(expected, actual);
        DiffElement diff = verifier.diff();

        assertNotNull("Should have found a difference", diff);

        System.out.println("Diff: " + diff.toString());

        //         TODO: figure out how to compare these
        return diff;
    }

    @Test
    public void testDiffDsMissingLeft() throws Exception {
        DiffElement diff = testDiffDSMissing(TEST_DS_PATH, TEST_DS_MISSING_LINE_PATH);

        assertNull(diff.getExpected());
        assertNotNull(diff.getActual());
    }

    @Test
    public void testDiffDsMissingRight() throws Exception {
        DiffElement diff = testDiffDSMissing(TEST_DS_MISSING_LINE_PATH, TEST_DS_PATH);

        assertNotNull(diff.getExpected());
        assertNull(diff.getActual());
    }
}