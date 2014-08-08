package com.hftparser.readers;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;

@SuppressWarnings("UnusedAssignment")
@RunWith(JUnit4.class)
public class MarketOrderCollectionTest {

    @Test
    public void testInstantiate() {
        try {
            BuyOrders buyOrders = new BuyOrders();
            SellOrders sellOrders = new SellOrders();

        } catch (Throwable t) {
            fail("Exception thrown during instantiation: " + t.toString());
        }
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
    public void testSellOrders() {
        SellOrders sellOrders = new SellOrders();

        addElsAndTest(sellOrders);

        long[][] top1 = {{5, 2}};
        long[][] top2 = {{5, 2}, {10, 5}};
        long[][] top4 = {{5, 2}, {10, 5}, {11, 6}};

        checkArraysEqual(top1, sellOrders.topN(1));
        checkArraysEqual(top2, sellOrders.topN(2));
        checkArraysEqual(top4, sellOrders.topN(4));
    }


    @Test
    public void testBuyOrders() {
        BuyOrders buyOrders = new BuyOrders();

        addElsAndTest(buyOrders);

        long[][] top1 = {{11, 6}};
        long[][] top2 = {{11, 6}, {10, 5}};
        long[][] top4 = {{11, 6}, {10, 5}, {5, 2}};

        checkArraysEqual(top1, buyOrders.topN(1));
        checkArraysEqual(top2, buyOrders.topN(2));
        checkArraysEqual(top4, buyOrders.topN(4));
    }


    // --Commented out by Inspection START (7/31/14 6:09 PM):
    //	static void printArray(int[][] arr) {
    //		for(int i = 0; i < arr.length; i++) {
    //			for(int j = 0; j < arr[i].length; j++) {
    //				System.out.print(arr[i][j] + " ");
    //			}
    //			System.out.println();
    //		}
    //	}
    // --Commented out by Inspection STOP (7/31/14 6:09 PM)

    void checkArraysEqual(long[][] expected, long[][] actual) {
        // System.out.println("Checking array: ");
        // printArray(actual);

        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i].length, actual[i].length);
            assertArrayEquals(expected[i], actual[i]);
        }
    }

}
