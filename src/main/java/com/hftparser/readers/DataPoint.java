package com.hftparser.readers;

import java.util.Arrays;

// maybe this should go in with the writers
public class DataPoint {
    private final long[][] buy;
    private final long[][] sell;
    private final long timeStamp;
    private final long seqNum;
    private final String ticker;

    public DataPoint(String _ticker, long[][] _buy, long[][] _sell, long _timeStamp, long _seqNum) {
        ticker = _ticker;
        buy = _buy;
        sell = _sell;
        timeStamp = _timeStamp;
        seqNum = _seqNum;
    }


    public String getTicker() {
        return ticker;
    }


    public void print() {
        System.out.println(this.toString());
    }


    public boolean equals(DataPoint other) {
        if (other == null) {
            return false;
        }

        boolean res = other.ticker.equals(ticker) &&
                (other.seqNum == seqNum) &&
                (other.timeStamp == timeStamp) &&
                Arrays.deepEquals(other.buy, buy) &&
                Arrays.deepEquals(other.sell, sell);

        //        System.out.println("Testing pair:");
        //
        //        print();
        //        other.print();
        //
        //        System.out.println("Equal?" + res);

        return res;
    }

    public WritableDataPoint getWritable() {
        return new WritableDataPoint(buy, sell, timeStamp, seqNum);

    }

    @Override
    public String toString() {
        return "DataPoint{" +
                "buy=" + Arrays.deepToString(buy) +
                ", sell=" + Arrays.deepToString(sell) +
                ", timeStamp=" + timeStamp +
                ", seqNum=" + seqNum +
                ", ticker='" + ticker + '\'' +
                '}';
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long[][] getBuy() {
        return buy;
    }

    public long[][] getSell() {
        return sell;
    }
}
