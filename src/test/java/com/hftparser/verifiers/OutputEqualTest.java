package com.hftparser.verifiers;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.readers.AbstractWritableDataPoint;
import com.hftparser.readers.PythonWritableDataPoint;
import com.hftparser.readers.WritableDataPoint;
import com.hftparser.writers.HDF5CompoundDSBridgeBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by patrick on 8/4/14.
 */
@Ignore
public class OutputEqualTest {
    String MY_DATA = "src/test/resources/java-out.h5";
    String PYTHON_DATA = "src/test/resources/python-out.h5";

    @Test
    public void runTest() {
        IHDF5Writer expectedWriter = HDF5Factory.open(PYTHON_DATA);
        IHDF5Writer actualWriter = HDF5Factory.open(MY_DATA);

        HDF5CompoundDSBridgeBuilder<PythonWritableDataPoint> expectedBuilder = new HDF5CompoundDSBridgeBuilder<>
                (expectedWriter);
        expectedBuilder.setAnonTypeFromInferred(PythonWritableDataPoint.class);

        HDF5CompoundDSBridgeBuilder<AbstractWritableDataPoint> actualBuilder = new HDF5CompoundDSBridgeBuilder<>(actualWriter);
        actualBuilder.setTypeFromInferred(AbstractWritableDataPoint.class);

//        HDF5CompoundVerifier<WritableDataPoint> verifier = new HDF5CompoundVerifier<>(actualWriter, expectedWriter,
//                                                                                      WritableDataPoint.class);

        HDF5CompoundVerifier<PythonWritableDataPoint, AbstractWritableDataPoint> verifier = new
                HDF5CompoundVerifier<>(expectedWriter, actualWriter, expectedBuilder, actualBuilder);

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

        for (String symbol : symbols) {
            System.out.println("Running for symbol: " + symbol);

            DiffElement diff = verifier.diff(symbol);
            System.out.println("Got diff: " + (diff != null ? diff.deepToString() : null));
            if (diff != null) {
                diffs.add(diff);
            }

        }

        assertEquals(0, diffs.size());
    }

}
