package com.hftparser.readers;


import com.hftparser.config.ArcaParserConfig;
import com.hftparser.containers.WaitFreeQueue;
import org.apache.commons.lang.mutable.MutableBoolean;

import java.util.*;


class Order {
    final long price;
    final long quantity;

    public Order(long _price, long _quantity) {
        price = _price;
        quantity = _quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Order order = (Order) o;

        if (price != order.price) {
            return false;
        }
        if (quantity != order.quantity) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "Order{" +
                "price=" + price +
                ", quantity=" + quantity +
                '}';
    }
}




public class ArcaParser extends AbstractParser implements Runnable {
    private final WaitFreeQueue<String> inQueue;
    private final WaitFreeQueue<DataPoint> outQueue;
    @SuppressWarnings("FieldCanBeLocal")
    private final String[] tickers;
    private volatile MutableBoolean pipelineError;

    // TODO: would it be faster to try to use eg an enum here?

    // TODO: according to stackoverflow.com/questions/81346/, we can
    // save time if we roll a MutableLong to use for qtys. we can do
    // this later if it's not fast enough
    private final Map<String, Map<OrderType, MarketOrderCollection>> ordersNow;

    // We're here abusing the fact that (at least within a day), there
    // are less than 2^64 orders, and saving just the end of the
    // 20-digit order reference number. This may break.
    private final Map<String, Map<Long, Order>> orderHistory;

    // Split CSVs on commas
    private final String INPUT_SPLIT = ",";
    //    private final Pattern SPLITTER;
    //    private final Pattern ROW_NEEDED;


    // For modify, we need to pull at most 12 things out of record
    private final int IMPORTANT_SYMBOL_COUNT = 13;

    // like in Python -- this is our top N values in buy/sell prices we care about
    public static final int LEVELS = 10;

    @SuppressWarnings("FieldCanBeLocal")
    private final int INITIAL_ORDER_HISTORY_SIZE;
    private final int OUTPUT_PROGRESS_EVERY;

    private final Map<String, RecordType> recordTypeLookup;


    public ArcaParser(String[] _tickers,
                      WaitFreeQueue<String> _inQueue,
                      WaitFreeQueue<DataPoint> _outQueue,
                      MarketOrderCollectionFactory collectionFactory,
                      ArcaParserConfig config,
                      MutableBoolean pipelineError) {

        tickers = _tickers;
        inQueue = _inQueue;
        outQueue = _outQueue;
        this.pipelineError = pipelineError;

        INITIAL_ORDER_HISTORY_SIZE = config.getInitial_order_history_size();
        OUTPUT_PROGRESS_EVERY = config.getOutput_progress_every();

        ordersNow = new HashMap<>(tickers.length);
        orderHistory = new NonnullHashMap<>(tickers.length);

        // First we initialize with empty hashmaps
        for (String ticker : tickers) {
            Map<OrderType, MarketOrderCollection> toAdd = new NonnullHashMap<>();
            toAdd.put(OrderType.Buy, collectionFactory.buildBuy());
            toAdd.put(OrderType.Sell, collectionFactory.buildSell());
            ordersNow.put(ticker, toAdd);
            orderHistory.put(ticker, new NonnullHashMap<Long, Order>());
        }

        // Set up a lookup table for our recordTypes
        recordTypeLookup = new HashMap<>(4);
        recordTypeLookup.put("A", RecordType.Add);
        recordTypeLookup.put("M", RecordType.Modify);
        recordTypeLookup.put("D", RecordType.Delete);
    }

    public ArcaParser(String[] _tickers,
                      WaitFreeQueue<String> _inQueue,
                      WaitFreeQueue<DataPoint> _outQueue,
                      MarketOrderCollectionFactory collectionFactory) {

        this(_tickers, _inQueue, _outQueue, collectionFactory, ArcaParserConfig.getDefault(), new MutableBoolean());

    }


    public void setStartCalendar(Calendar startDate) {
        Record.setStartCalendar(startDate);
    }

    public TimeZone getDefaultTz() {
        return Record.getDefaultTz();
    }

    public long getStartTimestamp() {
        return Record.getStartTimestamp();
    }


    private void processRecord(Record record) {
        Map<OrderType, MarketOrderCollection> ordersForTicker = ordersNow.get(record.getTicker());

        MarketOrderCollection toUpdate = ordersForTicker.get(record.getOrderType());

        try {
            record.process(toUpdate, orderHistory);
        } catch (KeyError error) {
            System.out.println("Error parsing record: " + record);
            System.out.println(error.getMessage());
//            We need to let it bubble up because the top level lets everyone else know that we died
            throw error;
        }

        MarketOrderCollection buyOrders = ordersForTicker.get(OrderType.Buy);
        MarketOrderCollection sellOrders = ordersForTicker.get(OrderType.Sell);

        //        System.out.println("buy orders dirty? " + buyOrders.isDirty());
        //        System.out.println("sell orders dirty? " + sellOrders.isDirty());

        if ((buyOrders.isDirty() || sellOrders.isDirty())) {
            long[][] toBuyNow = buyOrders.topN();
            long[][] toSellNow = sellOrders.topN();


            DataPoint toPush = new ValidDataPoint(record.getTicker(),
                                             toBuyNow,
                                             toSellNow,
                                             record.getTimeStamp(),
                                             record.getSeqNum());

//            System.out.println("About to push a DataPoint:" + toPush.toString());

            // spin until we successfully push
            //noinspection StatementWithEmptyBody
            while (!outQueue.enq(toPush) && !pipelineError.booleanValue()) {
            }
        }
    }



    private AddRecord parseAdd(String[] asSplit) {
        if (ordersNow.containsKey(AddRecord.tickerFromSplit(asSplit))) {
            return new AddRecord(asSplit);
        } else {
            return null;
        }
    }


    private DeleteRecord parseDelete(String[] asSplit) {
        if (ordersNow.containsKey(DeleteRecord.tickerFromSplit(asSplit))) {
            return new DeleteRecord(asSplit);
        } else {
            return null;
        }
    }


    private ModifyRecord parseModify(String[] asSplit) {
        if (ordersNow.containsKey(ModifyRecord.tickerFromSplit(asSplit))) {
            return new ModifyRecord(asSplit);
        } else {
            return null;
        }
    }

    private void purgeFailingTicker(String ticker) {
        ordersNow.remove(ticker);
        outQueue.enq(new PoisonDataPoint(ticker));
    }

    public void run() {
        String toParse;
        String[] asSplit = null;
        int linesSoFar = 0;

        RecordType recType;

        try {
            // Stop work when the Gzipper tells us to, once we've pulled
            // everything out of his queue
            while (!pipelineError.booleanValue() && (inQueue.acceptingOrders || !inQueue.isEmpty())) {
                // Work if we got something from the queue, otherwise spin
                if ((toParse = inQueue.deq()) != null) {
                    //                System.out.println("Parser got a line:" + toParse);
                    linesSoFar++;

                    if (linesSoFar % OUTPUT_PROGRESS_EVERY == 0) {
                        System.out.printf("Parsed %d lines\n", linesSoFar);
                    }

                    asSplit = toParse.split(INPUT_SPLIT, IMPORTANT_SYMBOL_COUNT + 1);
//
//                    System.out.println("asSplit: " + Arrays.toString(asSplit));

                    // Also note that containsKey is O(1)
                    if ((recType = recordTypeLookup.get(asSplit[0])) == null) {
                        // skip if it's not add, modify, delete
                        continue;
                    }
//
//                    System.out.println("asSplit: " + Arrays.toString(asSplit));

                    // Also note that containsKey is O(1)
                    Record toProcess = null;
                    switch (recType) {
                        case Add:
                            toProcess = parseAdd(asSplit);
                            break;
                        case Modify:
                            toProcess = parseModify(asSplit);
                            break;
                        case Delete:
                            toProcess = parseDelete(asSplit);
                            break;
                    }

                    if (toProcess != null) {
                        try {
                            processRecord(toProcess);
                        } catch (KeyError keyError) {
                            System.out.println(keyError.getMessage());
                            keyError.printStackTrace();
                            System.out.println(
                                    "Attempted to process an invalid order. This is probably due to a duplicate add. " +
                                            "Failing record: " + toProcess.toString());
                            System.out.println("To troubleshoot, check the input file for malformed data.");
                            System.out.println(
                                    "The symbol " + toProcess.getTicker() + " will be removed from the output file.");
                            purgeFailingTicker(toProcess.getTicker());
                        }
                    }



                    }

            }
        } catch (Throwable throwable) {
            pipelineError.setValue(true);
            System.out.println("Parser failed. Stacktrace:");
            System.out.println("Failling row: " + (asSplit == null ? "null" : Arrays.toString(asSplit)));
            throwable.printStackTrace();
            throw throwable;
        } finally {
            outQueue.acceptingOrders = false;
        }

    }

}
