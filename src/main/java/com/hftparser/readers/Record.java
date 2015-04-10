package com.hftparser.readers;

import com.hftparser.containers.MarketOrderCollection;

import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/**
 * Responsible for parsing and processing a single row of the input CSV
 */
abstract class Record {
    // offset for integer part of the price -- we pass it around as a
    // float to avoid floating point error. So we make the int part
    // bigger by this amount, and add in the float part in the
    // lower-order digits.
    private final int PRICE_OFFSET_EXP = 6;
    private final int PRICE_INTEGER_OFFSET = (int) Math.pow(10, 6);
    private static final TimeZone DEFAULT_TZ = TimeZone.getTimeZone("America/New_York");

    private static long startTimestamp = 0;
    protected String ticker;
    protected long seqNum; // need 9 digits
    protected long refNum; // need 20, but just taking last 19
    protected long timeStamp;
    protected OrderType orderType;

    // We set this in the parent class
    protected TickerOrderHistory orderHistory;

    public static void setStartCalendar(Calendar startDate) {
        startDate.setTimeZone(DEFAULT_TZ);
        startTimestamp = startDate.getTimeInMillis() * 1000;
    }

    public static long getStartTimestamp() {
        return startTimestamp;
    }

    public static TimeZone getDefaultTz() {
        return DEFAULT_TZ;
    }

    public String getTicker() {
        return ticker;
    }

    public static void setStartTimestamp(long startTimestamp) {
        Record.startTimestamp = startTimestamp;
    }

    protected long makeRefNum(String refNumStr) {
        int charsToTake = Math.min(19, refNumStr.length());
        return Long.parseLong(refNumStr.substring(0, charsToTake));
    }


    protected OrderType makeOrderType(String toParse) {
        return toParse.equals("B") ? OrderType.Buy : OrderType.Sell;
    }

    protected Long makePrice(String priceString) {
        String[] parts = priceString.split("\\.");
        long floatPart;
        long intPart;
        int sizeOfFloatPart = 0;

        intPart = safeParse(parts[0]);

        if (parts.length == 2) {
            floatPart = Long.parseLong(parts[1]);
            sizeOfFloatPart = parts[1].length();
        } else {
            floatPart = 0;
        }

        if (sizeOfFloatPart > 6) {
            throw new NumberFormatException("Can't have more than 6 decimal places. Got: " + priceString);
        } else {
            return intPart * PRICE_INTEGER_OFFSET +
                    (long) Math.pow(10, PRICE_OFFSET_EXP - sizeOfFloatPart) * floatPart;
        }
    }

/*
    ArcaBook apparently uses an empty millisecond field to denote things that happen at an offset of 0 ms from the
    second value. This is not documented.
 */
    protected long makeTimestamp(String seconds, String ms) {
        Long parsedMs;
        if (ms.isEmpty()) {
            parsedMs = 0l;
        } else {
            parsedMs = Long.parseLong(ms) * 1000l;
        }
        return Long.parseLong(seconds) * 1000000l + parsedMs + startTimestamp;
    }


    protected Long safeParse(String toParse) {
        if (toParse.isEmpty()) {
            return 0l;
        } else {
            return Long.parseLong(toParse);
        }
    }

    public void process(MarketOrderCollection toUpdate, Map<String, TickerOrderHistory> orderHistories) {
        // Skip if seqnum is bad
        if (prepare(orderHistories)) {
            processTemplateMethod(toUpdate, orderHistories);
        } else {
            System.out.println(
                    "WARNING: Ignoring an out-of-order seqNum. Check input data. Igoring for: " + this.toString());
        }
    }

    protected abstract void processTemplateMethod(MarketOrderCollection toUpdate,
                                                  Map<String, TickerOrderHistory> orderHistories);

    protected boolean prepare(Map<String, TickerOrderHistory> orderHistories) {
        orderHistory = orderHistories.get(ticker);
        return orderHistory.updateSeqNum(seqNum);
    }

    public OrderType getOrderType() {
        return orderType;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getSeqNum() {
        return seqNum;
    }

    public long getRefNum() {
        return refNum;
    }

    @Override
    public String toString() {
        return "Record{" +
                "ticker='" + ticker + '\'' +
                ", seqNum=" + seqNum +
                ", refNum=" + refNum +
                ", orderType=" + orderType +
                '}';
    }
}


// type, seq, refNum, _, b/s, count, ticker, price, sec, ms, _, _, _
// type is A(dd) | M(odify) | D(elete)
// b/s is B(uy) | S(ell)
// 0:type, 1:seq, 2:refNum, _, 4:b/s, 5:count, 6:ticker, 7:price, 8:sec,
// 9:ms, _, _, _
class AddRecord extends Record {
    private final int qty; // 8 digits
    private final Long price;

    public AddRecord(String[] asSplit) {
        ticker = tickerFromSplit(asSplit);
        seqNum = Long.parseLong(asSplit[1]); // drop last char out of 20
        refNum = makeRefNum(asSplit[2]);
        orderType = makeOrderType(asSplit[4]);
        qty = Integer.parseInt(asSplit[5]);
        price = makePrice(asSplit[7]);
        timeStamp = makeTimestamp(asSplit[8], asSplit[9]);
    }

    //    Find the ticker for this record without building a new one, so we can test if we need to parse it
    public static String tickerFromSplit(String[] asSplit) {
        return asSplit[6];
    }

    @Override
    protected void processTemplateMethod(MarketOrderCollection toUpdate,
                                         Map<String, TickerOrderHistory> orderHistories) {
        Long oldQty;

        if ((oldQty = toUpdate.get(price)) == null) {
            oldQty = 0l;
        }

        toUpdate.put(price, qty + oldQty);
        // We just added something twice -- time to fail
        if (orderHistory.put(refNum, new Order(price, qty)) != null) {
            throw new DuplicateAddError(this);
        }
    }

    public Long getPrice() {
        return price;
    }

    public int getQty() {
        return qty;
    }

    @Override
    public String toString() {
        return "AddRecord{" +
                "qty=" + qty +
                ", price=" + price +
                "} " + super.toString();
    }

    /**
     * Error to report adds with duplicate refNums
     */
    public static class DuplicateAddError extends RuntimeException {
        private final Record failing;

        public DuplicateAddError(Record failing) {
            super("Failed attempting to parse record: " + failing +
                          ", which is a duplicate. To troubleshoot, check the input file for malformed data.");
            this.failing = failing;
        }

        public Record getFailing() {
            return failing;
        }
    }
}

// 1:seq, 2:order id, 3:seconds, 4:ms, 5:ticker, 9:type
class DeleteRecord extends Record {
    public DeleteRecord(String[] asSplit) {
        ticker = tickerFromSplit(asSplit);
        seqNum = Long.parseLong(asSplit[1]);
        refNum = makeRefNum(asSplit[2]);
        timeStamp = makeTimestamp(asSplit[3], asSplit[4]);
        orderType = makeOrderType(asSplit[9]);
    }

    public static String tickerFromSplit(String[] asSplit) {
        return asSplit[5];
    }

    @Override
    protected void processTemplateMethod(MarketOrderCollection toUpdate,
                                         Map<String, TickerOrderHistory> orderHistories) {
        Order toDelete = orderHistory.get(refNum);
        Long currentQty = toUpdate.get(toDelete.price);

        toUpdate.put(toDelete.price, currentQty - toDelete.quantity);
        orderHistory.remove(refNum);
    }

    @Override
    public String toString() {
        return "DeleteRecord{} " + super.toString();
    }
}

// 1: seq, 2:ref num, 3:qty, 4:price, 5:sec, 6:ms, 7:ticker, b/s:11,
class ModifyRecord extends Record {
    private final int qty; // 8 digits
    private final Long price;

    public ModifyRecord(String[] asSplit) {
        seqNum = Long.parseLong(asSplit[1]);
        refNum = makeRefNum(asSplit[2]);
        qty = Integer.parseInt(asSplit[3]);
        price = makePrice(asSplit[4]);
        timeStamp = makeTimestamp(asSplit[5], asSplit[6]);
        orderType = makeOrderType(asSplit[11]);
        ticker = tickerFromSplit(asSplit);
    }

    public static String tickerFromSplit(String[] asSplit) {
        return asSplit[7];
    }

    @Override
    protected void processTemplateMethod(MarketOrderCollection toUpdate,
                                         Map<String, TickerOrderHistory> orderHistories) {
        Order changedOrder = new Order(price, qty);
        TickerOrderHistory tickerHistory = orderHistories.get(ticker);
        Order toModify = tickerHistory.get(refNum);

        //        otherwise we write out too many records
        if (changedOrder.equals(toModify)) {
            return;
        }

        Long currentQtyOfOldPrice = toUpdate.get(toModify.price);

        toUpdate.put(toModify.price, currentQtyOfOldPrice - toModify.quantity);

        // Note that we need to do this AFTER we delete, otherwise we
        // can grab the wrong value
        Long currentQtyOfNewPrice = toUpdate.get(price);
        Long qtyOfNewPriceToAdd = currentQtyOfNewPrice == null ? 0 : currentQtyOfNewPrice;


        tickerHistory.put(refNum, changedOrder);
        toUpdate.put(price, qty + qtyOfNewPriceToAdd);
    }

    public int getQty() {
        return qty;
    }

    public Long getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "ModifyRecord{" +
                "qty=" + qty +
                ", price=" + price +
                "} " + super.toString();
    }
}