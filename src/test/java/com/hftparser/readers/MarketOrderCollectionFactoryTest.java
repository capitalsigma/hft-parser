package com.hftparser.readers;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MarketOrderCollectionFactoryTest {
    MarketOrderCollectionFactory factory;

    @Before
    public void setUp() {
        factory = new MarketOrderCollectionFactory();
        factory.setMaxKeyCount(4);
        factory.setDoCaching(false);
    }

    void addElsAndTest(MarketOrderCollection orders) {
        orders.put(10l, 5l);
        orders.put(11l, 6l);
        orders.put(5l, 2l);

        assertEquals((long) orders.get(10l), 5l);
        assertEquals((long) orders.get(5l), 2l);
        assertEquals((long) orders.get(11l), 6l);
    }

    @Test
    public void testBuildSell() throws Exception {
        MarketOrderCollection sellOrders = factory.buildSell();

        addElsAndTest(sellOrders);

        long[][] top1 = {{5, 2}};
        long[][] top2 = {{5, 2}, {10, 5}};
        long[][] top4 = {{5, 2}, {10, 5}, {11, 6}};

        checkArraysEqual(top1, sellOrders.topN(1));
        checkArraysEqual(top2, sellOrders.topN(2));
        checkArraysEqual(top4, sellOrders.topN(4));
    }

    public void checkArraysEqual(long[][] expected, long[][] actual) {
        assertTrue(Arrays.deepEquals(expected, actual));
    }

    @Test
    public void testBuildBuy() throws Exception {
        MarketOrderCollection buyOrders = factory.buildBuy();

        addElsAndTest(buyOrders);

        long[][] top1 = {{11, 6}};
        long[][] top2 = {{11, 6}, {10, 5}};
        long[][] top4 = {{11, 6}, {10, 5}, {5, 2}};

        checkArraysEqual(top1, buyOrders.topN(1));
        checkArraysEqual(top2, buyOrders.topN(2));
        checkArraysEqual(top4, buyOrders.topN(4));

    }
}