package com.hftparser.readers;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MarketOrderCollectionCacheTest {
    MarketOrderCollection orderCollection;

    @Before
    public void setUp() throws Exception {
        MarketOrderCollectionFactory factory = new MarketOrderCollectionFactory();
        factory.setMaxKeyCount(2);
        factory.setDoCaching(true);

        orderCollection = factory.buildBuy();
    }

//    @Test
//    public void testTopN() throws Exception {
//
//    }

    @Test
    public void testGet() throws Exception {
        orderCollection.put(10, 5);

        assertEquals(orderCollection.get(10), (Integer) 5);
    }

    @Test
    public void testIsDirty() throws Exception {
        assertTrue(orderCollection.isDirty());

        orderCollection.put(10, 5);

        assertTrue(orderCollection.isDirty());

        orderCollection.put(11, 5);

        assertTrue(orderCollection.isDirty());

        orderCollection.put(12, 5);

        assertTrue(orderCollection.isDirty());

        orderCollection.topN();

        assertFalse(orderCollection.isDirty());

        System.out.println("orderCollection: " + orderCollection.toString());

        orderCollection.put(5, 5);

        assertFalse(orderCollection.isDirty());

        orderCollection.put(6, 7);
        orderCollection.put(7, 8);

        System.out.println("orderCollection: " + orderCollection.toString());

        orderCollection.put(10, 3);

        System.out.println("orderCollection: " + orderCollection.toString());

        assertFalse(orderCollection.isDirty());

        orderCollection.put(12, 0);

        assertTrue(orderCollection.isDirty());

        orderCollection.topN();

        assertFalse(orderCollection.isDirty());

        orderCollection.put(13, 1);

        assertTrue(orderCollection.isDirty());
    }
}