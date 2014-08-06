package com.hftparser.readers;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.hftparser.containers.WaitFreeQueue;

// TODO: add a test to check a modify followed by a delete
@SuppressWarnings("UnusedAssignment")
@RunWith(JUnit4.class)
public class ArcaParserTest {
	private final String[] TEST_TICKERS = {"FOO", "BAR"};

	private final String TEST_ADD_BUY1 =
		"A,1,12884901908,B,B,1000,FOO,2.75,28800,737,B,AARCA,";
	private final String TEST_ADD_BUY2 =
		"A,1,12884902050,B,B,3200,FOO,0.98,28800,737,B,AARCA,";

	private final String TEST_ADD_SELL1 =
		"A,8,12884902687,B,S,30000,FOO,0.02,28800,739,B,AARCA,";
	String TEST_ADD_SELL2 =
		"A,12,12884902091,B,S,200000,BAR,0.0195,28800,740,B,AARCA,";

	String TEST_DELETE_BUY1 =
		"D,2,12884901908,28800,857,FOO,B,B,AARCA,B,";

	private final String TEST_MODIFY_BUY1 =
		"M,43,12884901908,900,0.3825,29909,390,FOO,B,B,AARCA,B,";
	private final String TEST_MODIFY_BUY2 =
		"M,2,12884902050,3000,0.98,33643,922,FOO,B,B,AARCA,B,";

    private final String TEST_WHOLE =
        "A,1,12884901908,B,B,1000,FOO,275,28800,737,B,AARCA,";

    private final String TEST_OVERFLOW = "A,24,4503612512341875,P,S,100,FOO,99999.9844,31830,137,E,AARCA,";

    MarketOrderCollectionFactory collectionFactory;
    private WaitFreeQueue<String> inQ;
    private WaitFreeQueue<DataPoint> outQ;


    @Before
    public void setUp() {
        collectionFactory = new MarketOrderCollectionFactory();
        inQ = new WaitFreeQueue<>(5);
        outQ = new WaitFreeQueue<>(5);
    }

    @Test
    public void testOverflow() throws Exception {
        inQ.enq(TEST_OVERFLOW);
        ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ, collectionFactory);

        runParserThread(parser);

        long[][] expectedSells = new long[][]{{99999984400l, 100l}};

        DataPoint expected = new DataPoint("FOO", new long[][]{}, expectedSells, 31830137, 24);
        DataPoint actual = outQ.deq();

        System.out.println("Got actual: " + actual.toString());

        assertTrue(actual.equals(expected));

    }


    @Test
    public void testModifyThenDelete() throws Exception {

        ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ, collectionFactory);

        inQ.enq(TEST_ADD_BUY1);
        inQ.enq(TEST_MODIFY_BUY1);
        inQ.enq(TEST_DELETE_BUY1);

        runParserThread(parser);

        long[][] expectedOneBuy = {{2750000, 1000}};

        long[][] expectedTwoBuy = {{382500, 900}};

        DataPoint buyExpected = new DataPoint("FOO", expectedOneBuy, new long[][] {}, 28800737, 1);

        DataPoint modifyExpected = new DataPoint("FOO", expectedTwoBuy, new long[][] {}, 29909390, 43);

        DataPoint deleteExpected = new DataPoint("FOO", new long[][]{}, new long[][]{}, 28800857, 2);

        assertTrue(outQ.deq().equals(buyExpected));
        assertTrue(outQ.deq().equals(modifyExpected));
        assertTrue(outQ.deq().equals(deleteExpected));
    }

    private void runParserThread(ArcaParser parser) throws InterruptedException {
        Thread runThread = new Thread(parser);
        runThread.start();
        Thread.sleep(100);
    }

    @Test
    public void testIdenticalModifiesCaching() throws Exception {
        collectionFactory.setDoCaching(true);

        WaitFreeQueue<String> inQ = new WaitFreeQueue<>(5);
        WaitFreeQueue<DataPoint> outQ = new WaitFreeQueue<>(5);

        inQ.enq(TEST_ADD_BUY1);
        inQ.enq(TEST_MODIFY_BUY1);
        inQ.enq(TEST_MODIFY_BUY1);

        ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ, collectionFactory);

        runParserThread(parser);

        //        make sure we didn't emit an extra record for the duplicate modify
        DataPoint one = outQ.deq();
        DataPoint two = outQ.deq();
        DataPoint three = outQ.deq();

        System.out.println("one: " + one.toString());
        System.out.println("two: " + two.toString());
        System.out.println("three: " + (three != null ? three.toString() : null));

        assertNotNull(one);
        assertNotNull(two);
        assertNull(three);

    }

	@Test
	public void testInstantiate() {
		try {

            ArcaParser parser = new ArcaParser(TEST_TICKERS,
                                               new WaitFreeQueue<String>(3),
                                               new WaitFreeQueue<DataPoint>(3),
                                               collectionFactory);
        } catch (Throwable t) {
			fail("Exception thrown during instantiation: " +
				 t.toString());
		}
	}

	// ArcaParser buildParser(int capacity) {
	// 	return new ArcaParser(TEST_TICKERS,
	// 						  new WaitFreeQueue<String>(capacity),
	// 						  new WaitFreeQueue<DataPoint>(capacity));
	// }

    @Test
    public void testAddWhole() throws Exception {
        WaitFreeQueue<String> inQ = new WaitFreeQueue<>(5);
        WaitFreeQueue<DataPoint> outQ = new WaitFreeQueue<>(5);

        ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ, collectionFactory);

        inQ.enq(TEST_WHOLE);
        runParserThread(parser);

        long[][] expectedOneBuy = {
                {275000000, 1000}
        };

        DataPoint buy1Expected =
                new DataPoint("FOO", expectedOneBuy, new long[][] {}, 28800737, 1);

        assertTrue(buy1Expected.equals(outQ.deq()));

    }

	@Test
	public void testAdd() throws Exception {
//		WaitFreeQueue<String> inQ = new WaitFreeQueue<>(5);
//		WaitFreeQueue<DataPoint> outQ = new WaitFreeQueue<>(5);

		ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ, collectionFactory);

		inQ.enq(TEST_ADD_BUY1);
		inQ.enq(TEST_ADD_SELL1);
		inQ.enq(TEST_ADD_BUY2);


        runParserThread(parser);


        long[][] expectedOneBuy = {
                {2750000, 1000}
        };


        long[][] expectedTwoBuy = {
                {2750000, 1000},
                // {980000, 3200}
        };
        long[][] expectedTwoSell = {
                {20000, 30000}
        };


        long[][] expectedThreeBuy = {
                {2750000, 1000},
                {980000, 3200}
        };
        long[][] expectedThreeSell = {
                {20000, 30000}
        };


		DataPoint buy1Expected =
			new DataPoint("FOO", expectedOneBuy, new long[][] {}, 28800737, 1);

		DataPoint sell1Expected =
			new DataPoint("FOO", expectedTwoBuy, expectedTwoSell, 28800739, 8);

		DataPoint buy2Expected =
			new DataPoint("FOO", expectedThreeBuy, expectedThreeSell, 28800737, 1);

		assertTrue(buy1Expected.equals(outQ.deq()));
		assertTrue(sell1Expected.equals(outQ.deq()));
		assertTrue(buy2Expected.equals(outQ.deq()));
	}

	@Test
	public void testModify() throws Exception {
		WaitFreeQueue<String> inQ = new WaitFreeQueue<>(5);
		WaitFreeQueue<DataPoint> outQ = new WaitFreeQueue<>(5);

		ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ, collectionFactory);

		inQ.enq(TEST_ADD_BUY1);
		inQ.enq(TEST_ADD_BUY2);
		inQ.enq(TEST_MODIFY_BUY1);
		inQ.enq(TEST_MODIFY_BUY2);

        runParserThread(parser);

		// throw away the first two, we don't care (add is tested elsewhere)
		outQ.deq();
		outQ.deq();

		long[][] expectedOrders1Buy =
			{
				{980000, 3200},
				{382500, 900}
			};
        long[][] expectedEmptySell = {

		};

		long[][] expectedOrders2Buy =
			{
				{980000, 3000},
				{382500, 900},
			};

		DataPoint expected1 = new DataPoint("FOO", expectedOrders1Buy, expectedEmptySell,
											29909390, 43);

		DataPoint expected2 = new DataPoint("FOO", expectedOrders2Buy, expectedEmptySell,
											33643922, 2);

		DataPoint toTest1 = outQ.deq();
		DataPoint toTest2 = outQ.deq();

		System.out.println("TO TEST #1:");
		toTest1.print();
		System.out.println("TO TEST #2:");
		toTest2.print();

		boolean test1 = expected1.equals(toTest1);
		boolean test2 = expected2.equals(toTest2);

		System.out.println("Test 1: " + test1);
		System.out.println("Test 2: " + test2);

		assertTrue(test1);
		assertTrue(test2);
	}
}
