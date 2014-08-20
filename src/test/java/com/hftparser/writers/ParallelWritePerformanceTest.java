package com.hftparser.writers;

import ch.systemsx.cisd.hdf5.IHDF5Writer;
import com.hftparser.config.ConfigFactory;
import com.hftparser.config.HDF5CompoundDSBridgeConfig;
import com.hftparser.readers.WritableDataPoint;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by patrick on 8/20/14.
 */
@RunWith(JUnit4.class)
@Ignore
public class ParallelWritePerformanceTest {
    String TEST_CONFIG_SMALL = "src/test/resources/async-config.json";
    String TEST_CONFIG_BIG = "src/test/resources/test-parallel-write-big.json";
    String OUTFILE = "out.h5";
    String[] NAMES = new String[]{"foo", "bar", "baz", "bat", "bam", "bat", "bant", "eo", "is", "it", "int"};
    WritableDataPoint testPoint1 = new WritableDataPoint(new long[][]{{1, 2}}, new long[][]{{3, 4}}, 6, 10l);
    private IHDF5Writer writer;


    public HDF5CompoundDSBridgeBuilder<WritableDataPoint> buildBuilderForPath(String path) throws Exception {
        File file = new File(OUTFILE);
        ConfigFactory factory = ConfigFactory.fromPath(path);
        HDF5CompoundDSBridgeConfig config = factory.getHdf5CompoundDSBridgeConfig();


        writer = HDF5Writer.getWriter(file);
        HDF5CompoundDSBridgeBuilder<WritableDataPoint> dtBuilder =
                new HDF5CompoundDSBridgeBuilder<>(writer, config);

        dtBuilder.setTypeFromInferred(WritableDataPoint.class);

        return dtBuilder;
    }

    @Test
    public void testParallelFlush() throws Exception {

        HDF5CompoundDSBridgeBuilder<WritableDataPoint> builder = buildBuilderForPath(TEST_CONFIG_BIG);
        long start;
        long end;

        List<HDF5CompoundDSBridge<WritableDataPoint>> writers = getHdf5CompoundDSBridges(builder);

        start = System.nanoTime();

        for (HDF5CompoundDSBridge<WritableDataPoint> dsWriter : writers) {
            dsWriter.appendElement(testPoint1);
        }

        end = System.nanoTime();

        writer.close();

        System.out.println("Elapsed for parallel flush: " + (start - end));
    }

    @Test
    public void testSeqFlush() throws Exception {

        HDF5CompoundDSBridgeBuilder<WritableDataPoint> builder = buildBuilderForPath(TEST_CONFIG_SMALL);
        long start;
        long end;

        List<HDF5CompoundDSBridge<WritableDataPoint>> writers = getHdf5CompoundDSBridges(builder);

        start = System.nanoTime();

        for (HDF5CompoundDSBridge<WritableDataPoint> dsWriter : writers) {
            dsWriter.appendElement(testPoint1);
            dsWriter.flush();
        }

        end = System.nanoTime();

        System.out.println("Elapsed for seq flush: " + (start - end));
    }

    private List<HDF5CompoundDSBridge<WritableDataPoint>> getHdf5CompoundDSBridges
            (HDF5CompoundDSBridgeBuilder<WritableDataPoint> builder) {

        List<HDF5CompoundDSBridge<WritableDataPoint>> writers = new ArrayList<>();
        for (String name : NAMES) {
            System.out.println("Built...");

            writers.add(builder.build(new DatasetName(name, name)));
        }
        return writers;
    }


    @After
    public void tearDown() {
        writer.close();
    }

}
