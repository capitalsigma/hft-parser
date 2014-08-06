package com.hftparser.readers;

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
        orderCollection.put(10l, 5l);

        assertEquals(orderCollection.get(10l), (Long) 5l);
    }

    @Test
    public void testIsDirty() throws Exception {
        assertTrue(orderCollection.isDirty());

        orderCollection.put(10l, 5l);

        assertTrue(orderCollection.isDirty());

        orderCollection.put(11l, 5l);

        assertTrue(orderCollection.isDirty());

        orderCollection.put(12l, 5l);

        assertTrue(orderCollection.isDirty());

        orderCollection.topN();

        assertFalse(orderCollection.isDirty());

        System.out.println("orderCollection: " + orderCollection.toString());

        orderCollection.put(5l, 5l);

        assertFalse(orderCollection.isDirty());

        orderCollection.put(6l, 7l);
        orderCollection.put(7l, 8l);

        System.out.println("orderCollection: " + orderCollection.toString());

        orderCollection.put(10l, 3l);

        System.out.println("orderCollection: " + orderCollection.toString());

        assertFalse(orderCollection.isDirty());

        orderCollection.put(12l, 0l);

        assertTrue(orderCollection.isDirty());

        orderCollection.topN();

        assertFalse(orderCollection.isDirty());

        orderCollection.put(13l, 1l);

        assertTrue(orderCollection.isDirty());
    }
}