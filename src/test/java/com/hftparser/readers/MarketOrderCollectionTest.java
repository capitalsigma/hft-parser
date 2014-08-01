package com.hftparser.readers;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.hftparser.readers.BuyOrders;
import com.hftparser.readers.MarketOrderCollection;
import com.hftparser.readers.SellOrders;

@SuppressWarnings("UnusedAssignment")
@RunWith(JUnit4.class)
public class MarketOrderCollectionTest {

	@Test
	public void testInstantiate() {
		try {
			BuyOrders buyOrders = new BuyOrders();
			SellOrders sellOrders = new SellOrders();

		} catch (Throwable t) {
			fail("Exception thrown during instantiation: " +
						t.toString());
		}
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
	public void testSellOrders() {
		SellOrders sellOrders = new SellOrders();

		addElsAndTest(sellOrders);

		int[][] top1 = {{5, 2}};
		int[][] top2 = {{5, 2}, {10, 5}};
		int[][] top4 = {{5, 2}, {10, 5}, {11, 6}};

		checkArraysEqual(top1, sellOrders.topN(1));
		checkArraysEqual(top2, sellOrders.topN(2));
		checkArraysEqual(top4, sellOrders.topN(4));
	}


	@Test
	public void testBuyOrders() {
		BuyOrders buyOrders = new BuyOrders();

		addElsAndTest(buyOrders);

		int[][] top1 = {{11, 6}};
		int[][] top2 = {{11, 6}, {10, 5}};
		int[][] top4 = {{11, 6}, {10, 5}, {5, 2}};

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

	void checkArraysEqual(int[][] expected, int[][] actual) {
		// System.out.println("Checking array: ");
		// printArray(actual);

		assertEquals(expected.length, actual.length);
		for(int i = 0; i < expected.length; i++) {
			assertEquals(expected[i].length, actual[i].length);
			assertArrayEquals(expected[i], actual[i]);
		}
	}

}
