package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.readers.WritableDataPoint;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by patrick on 8/4/14.
 */
public class OutputEqualTest {
    String MY_DATA = "src/test/resources/java-out.h5";
    String PYTHON_DATA = "src/test/resources/python-out.h5";

    @Test
    public void runTest() {
        IHDF5Writer expectedWriter = HDF5Factory.open(PYTHON_DATA);
        IHDF5Writer actualWriter = HDF5Factory.open(MY_DATA);

        HDF5CompoundVerifier<WritableDataPoint> verifier = new HDF5CompoundVerifier<>(actualWriter, expectedWriter,
                                                                                      WritableDataPoint.class);

        DiffElement diff = verifier.diff();

        System.out.println("Got diff: " + (diff != null ? diff.deepToString() : null));
        assertNull(diff);
    }

}
