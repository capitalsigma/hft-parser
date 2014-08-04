package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HDF5CompoundVerifierTest {
    String TEST_DS_PATH = "src/test/resources/only-aapl.h5";
    String TEST_DS_MISSING_LINE_PATH = "src/test/resources/only-aapl-last-line-dropped.h5";
    HDF5CompoundVerifier verifier;


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
        IHDF5Reader actualReader = HDF5Factory.openForReading(TEST_DS_PATH);
        IHDF5Reader expectedReader = HDF5Factory.openForReading(TEST_DS_PATH);
        verifier = new HDF5CompoundVerifier(actualReader, expectedReader);
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

    @Test
    public void testDiffDSMissing() throws Exception {
        setVerifierForPaths(TEST_DS_MISSING_LINE_PATH, TEST_DS_PATH);
        DiffElement diff = verifier.diff();

        assertNotNull(diff);

        System.out.println("Diff: " + diff.toString());

        assertNull(diff.getExpected());
        assertNotNull(diff.getActual());

//         TODO: figure out how to compare these
    }
}