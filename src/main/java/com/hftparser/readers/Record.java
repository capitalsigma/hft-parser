package com.hftparser.readers;

import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by patrick on 1/28/15.
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
            Long toRet = intPart * PRICE_INTEGER_OFFSET +
                    (long) Math.pow(10, PRICE_OFFSET_EXP - sizeOfFloatPart) * floatPart;

            return toRet;
        }
    }

/*
    ArcaBook apparently uses an empty millisecond field to denote things that happen at an offset of 0 ms from the
    second value. This is not documented.
 */
    protected long makeTimestamp(String seconds, String ms) {
        Long parsedMs;
        if (ms.length() == 0) {
            parsedMs = 0l;
        } else {
            parsedMs = Long.parseLong(ms) * 1000l;
        }
        return Long.parseLong(seconds) * 1000000l + parsedMs + startTimestamp;
    }


    protected Long safeParse(String toParse) {
        if (toParse.length() == 0) {
            return 0l;
        } else {
            return Long.parseLong(toParse);
        }
    }

    abstract public void process(MarketOrderCollection toUpdate, Map<String, Map<Long, Order>> orderHistory);


    public OrderType getOrderType() {
        return orderType;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getSeqNum() {
        return seqNum;
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
    private int qty; // 8 digits
    private Long price;

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
    public void process(MarketOrderCollection toUpdate,
                        Map<String, Map<Long, Order>> orderHistory) {
        Long oldQty;

        if ((oldQty = toUpdate.get(price)) == null) {
            oldQty = 0l;
        }

        toUpdate.put(price, qty + oldQty);
        orderHistory.get(ticker).put(refNum, new Order(price, qty));
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
    public void process(MarketOrderCollection toUpdate, Map<String, Map<Long, Order>> orderHistory) {
        Map<Long, Order> tickerHistory = orderHistory.get(ticker);
        Order toDelete = tickerHistory.get(refNum);
        Long currentQty = toUpdate.get(toDelete.price);

//        System.out.printf("History: %s, toDelete: %s, toUpdate: %s, currentQty: %s\n",
//                          tickerHistory,
//                          toDelete,
//                          toUpdate,
//                          currentQty);

        toUpdate.put(toDelete.price, currentQty - toDelete.quantity);
        tickerHistory.remove(refNum);
    }
}

// 1: seq, 2:ref num, 3:qty, 4:price, 5:sec, 6:ms, 7:ticker, b/s:11,
class ModifyRecord extends Record {
    private int qty; // 8 digits
    private Long price;

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
    public void process(MarketOrderCollection toUpdate, Map<String, Map<Long, Order>> orderHistory) {
        Order changedOrder = new Order(price, qty);
        Map<Long, Order> tickerHistory = orderHistory.get(ticker);
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
}