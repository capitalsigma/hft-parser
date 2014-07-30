import com.hftparser.containers.WaitFreeQueueTest;
import com.hftparser.readers.ArcaParserTest;
import com.hftparser.readers.GzipReaderTest;
import com.hftparser.writers.HDF5CompoundDSBridgeTest;
import com.hftparser.writers.HDF5WriterTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        WaitFreeQueueTest.class,
        ArcaParserTest.class,
        GzipReaderTest.class,
        HDF5CompoundDSBridgeTest.class,
        HDF5WriterTest.class
})
public class RunTests {
//	public static void main(String argv[]) {
//		// JUnitCore.runClasses();
//		Result result = JUnitCore.runClasses(WaitFreeQueueTest.class);
//
//		for (Failure failure : result.getFailures()) {
//			System.out.println(failure.toString());
//		}
//	}
}
