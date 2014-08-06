package com.hftparser.main;

import org.junit.Test;

import java.util.Calendar;

import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;
import static org.hamcrest.CoreMatchers.*;

public class ParseRunTest {
    private final String TEST_FILENAME = "arcabookftp20101102.csv.gz";

    @Test
    public void testStartCalendarFromFilename() throws Exception {
        Calendar expected = Calendar.getInstance();
        expected.clear();
        expected.set(2010, Calendar.NOVEMBER, 2, 0, 0, 0);

        Calendar actual = ParseRun.startCalendarFromFilename(TEST_FILENAME);

        System.out.println("Got: " + actual.getTime().toString());
        System.out.println("Expected: " + expected.getTime().toString());
        System.out.println("Comparison? " + actual.compareTo(expected));

        assertThat(actual, equalTo(expected));
    }
}