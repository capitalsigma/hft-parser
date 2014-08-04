package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class HDF5CompoundVerifierTest {
    String TEST_H5_PATH = "src/test/resources/aapl-first-201.h5";
    String TEST_H5_MISSING_LINE_PATH = "src/test/resources/aapl-first-200.h5";
    String TEST_DS_PATH = "/AAPL/books";

    String TEST_H5_DIFFERENT_GROUPS = "src/test/resources/full-first-200.h5";

    HDF5CompoundVerifier<WritableDataPoint> verifier;


    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        verifier.closeReaders();
    }

    private void setVerifierForSame() throws Exception {
        setVerifierForPaths(TEST_H5_PATH, TEST_H5_PATH);

    }

    private void setVerifierForPaths(String expectedPath, String actualPath) throws Exception {
        IHDF5Writer expectedWriter = HDF5Factory.open(expectedPath);
        IHDF5Writer actualWriter = HDF5Factory.open(actualPath);

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

        System.out.println("Diff: " + diff.deepToString());

        assertNotNull(diff.getExpected());
        assertNotNull(diff.getActual());

        assertEquals(TEST_DS_PATH, diff.getExpected().getPath());
        assertEquals(TEST_DS_PATH, diff.getActual().getPath());

        assertEquals(diff.getIndex(), 200);


        //         TODO: figure out how to compare these
        return diff;
    }

    @Test
    public void testDiffDsMissingLeft() throws Exception {
        DiffElement diff = testDiffDSMissing(TEST_H5_PATH, TEST_H5_MISSING_LINE_PATH);
    }

    @Test
    public void testDiffDsMissingRight() throws Exception {
        DiffElement diff = testDiffDSMissing(TEST_H5_MISSING_LINE_PATH, TEST_H5_PATH);
    }

    @Test
    public void testDiffDsMissingEqual() throws Exception {
        DiffElement left = testDiffDSMissing(TEST_H5_PATH, TEST_H5_MISSING_LINE_PATH);
        DiffElement right = testDiffDSMissing(TEST_H5_MISSING_LINE_PATH, TEST_H5_PATH);

        assertEquals(left, right);
    }
}