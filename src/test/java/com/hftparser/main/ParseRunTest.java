package com.hftparser.main;

import com.hftparser.writers.HDF5Writer;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.integration.junit4.JMockit;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Arrays;
import java.util.Calendar;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

class MockParseRun extends MockUp<ParseRun> {
    static int invocationCount = 0;

    @Mock(invocations = 3)
    public static HDF5Writer runForSymbols(String[] symbols, boolean preserveForNextRun) {
        System.out.println("On invocation: " + invocationCount);
        switch (invocationCount++) {
            case 0:
                assertTrue(Arrays.deepEquals(symbols, new String[]{"A", "B", "C"}));
                assertFalse(preserveForNextRun);
                break;

            case 1:
                assertTrue(Arrays.deepEquals(symbols, (new String[]{"D", "E", "F"})));
                assertTrue(preserveForNextRun);
                break;

            case 2:
                assertTrue(Arrays.deepEquals(symbols, new String[]{"G", "H"}));
                assertTrue(preserveForNextRun);
                break;

            default:
                assertTrue(false);
        }

        return null;
    }

//    public static void runLoop(String[] symbols, Integer numPerRun) {
//        ParseRun.
//    }
}

@RunWith(JMockit.class)
public class ParseRunTest {
    private final String TEST_FILENAME = "arcabookftp20101102.csv.gz";
    private final String TEST_SYMBOLFILE = "src/test/resources/test-symbols.txt";

    @Test
    public void testStartCalendarFromFilename() throws Exception {
        Calendar expected = Calendar.getInstance();
        expected.clear();
        expected.set(2010, Calendar.NOVEMBER, 2, 0, 0, 0);
//        expected.setTimeZone(TimeZone.getTimeZone("UTC"));

        Calendar actual = ParseRun.startCalendarFromFilename(TEST_FILENAME);

        System.out.println("Got: " + actual.getTime().toString());
        System.out.println("Expected: " + expected.getTime().toString());
        System.out.println("Comparison? " + actual.compareTo(expected));


        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testParseSymbolFile() throws Exception {
        String[] expected = new String[]{"ABC", "DEF", "GHI", "JKL", "MNO"};
        File testFile = new File(TEST_SYMBOLFILE);
        String[] actual = ParseRun.parseSymbolFile(testFile);

        assertThat(actual, equalTo(expected));
    }

    @Test
    public void testrunLoop() throws Exception {
        String[] testSymbols = new String[]{"A", "B", "C", "D", "E", "F", "G", "H"};
        Integer numPerRun = 3;

        MockParseRun myMock = new MockParseRun();
        ParseRun myMockParseRun = myMock.getMockInstance();
        myMockParseRun.runLoop(testSymbols, numPerRun);
    }
}