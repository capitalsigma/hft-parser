package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.readers.WritableDataPoint;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by patrick on 8/4/14.
 */
//@Ignore
public class OutputEqualTest {
    String MY_DATA = "src/test/resources/java-out.h5";
    String PYTHON_DATA = "src/test/resources/python-out.h5";

    @Test
    public void runTest() {
        IHDF5Writer expectedWriter = HDF5Factory.open(PYTHON_DATA);
        IHDF5Writer actualWriter = HDF5Factory.open(MY_DATA);
        HDF5CompoundVerifier<WritableDataPoint> verifier = new HDF5CompoundVerifier<>(actualWriter, expectedWriter,
                                                                                      WritableDataPoint.class);
        String[] symbols = new String[]{
                "SPY", "DIA", "QQQ",
                "XLK", "XLF", "XLP", "XLE", "XLY", "XLV", "XLB",
                "VCR", "VDC", "VHT", "VIS", "VAW", "VNQ", "VGT", "VOX", "VPU",
                "XOM", "RDS", "BP",
                "HD", "LOW", "XHB",
                "MS", "GS", "BAC", "JPM", "C",
                "CME", "NYX",
                "AAPL", "MSFT", "GOOG", "CSCO",
                "GE", "CVX", "JNJ", "IBM", "PG", "PFE",
        };

        List<DiffElement> diffs = new ArrayList<>(symbols.length);

        for (int i = 0; i < symbols.length; i++) {
            DiffElement diff = verifier.diff(symbols[i]);
            System.out.println("Got diff: " + (diff != null ? diff.deepToString() : null));
            if (diff != null) {
                diffs.add(diff);
            }

        }

        assertEquals(0, diffs.size());
    }

}
