package com.hftparser.readers;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class NonnullHashMapTest {
    Map<Integer, Integer> map;

    @Before
    public void setUp() throws Exception {
        map = new NonnullHashMap<>();
    }

    @Test
    public void testGet() throws Exception {
        map.put(1, 1);
        assertThat(map.get(1), equalTo(1));
    }

    @Test
    public void testGetThrows() throws Exception {
        try {
            map.get(1);
        } catch (Throwable throwable) {
            assertTrue("Correctly threw.", true);
            return;
        }
        assertTrue("Did not throw!", false);
    }
}