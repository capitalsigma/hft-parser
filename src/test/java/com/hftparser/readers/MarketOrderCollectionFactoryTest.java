package com.hftparser.readers;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MarketOrderCollectionFactoryTest {
    MarketOrderCollectionFactory factory;

    @Before
    public void setUp() {
        factory = new MarketOrderCollectionFactory();
        factory.setMaxKeyCount(4);
        factory.setDoCaching(false);
    }

    void addElsAndTest(MarketOrderCollection orders) {
        orders.put(10, 5);
        orders.put(11, 6);
        orders.put(5, 2);

        assertEquals((int) orders.get(10), 5);
        assertEquals((int) orders.get(5), 2);
        assertEquals((int) orders.get(11), 6);
    }

    @Test
    public void testBuildSell() throws Exception {
        MarketOrderCollection sellOrders = factory.buildSell();

        addElsAndTest(sellOrders);

        int[][] top1 = {{5, 2}};
        int[][] top2 = {{5, 2}, {10, 5}};
        int[][] top4 = {{5, 2}, {10, 5}, {11, 6}};

        checkArraysEqual(top1, sellOrders.topN(1));
        checkArraysEqual(top2, sellOrders.topN(2));
        checkArraysEqual(top4, sellOrders.topN(4));
    }

    public void checkArraysEqual(int[][] expected, int[][] actual) {
        assertTrue(Arrays.deepEquals(expected, actual));
    }

    @Test
    public void testBuildBuy() throws Exception {
        MarketOrderCollection buyOrders = factory.buildBuy();

        addElsAndTest(buyOrders);

        int[][] top1 = {{11, 6}};
        int[][] top2 = {{11, 6}, {10, 5}};
        int[][] top4 = {{11, 6}, {10, 5}, {5, 2}};

        checkArraysEqual(top1, buyOrders.topN(1));
        checkArraysEqual(top2, buyOrders.topN(2));
        checkArraysEqual(top4, buyOrders.topN(4));

    }
}