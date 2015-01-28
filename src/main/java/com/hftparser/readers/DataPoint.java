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


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataPoint dataPoint = (DataPoint) o;

        return dataPoint.ticker.equals(ticker) &&
                (dataPoint.seqNum == seqNum) &&
                (dataPoint.timeStamp == timeStamp) &&
                Arrays.deepEquals(dataPoint.buy, buy) &&
                Arrays.deepEquals(dataPoint.sell, sell);
    }

    @Override
    public int hashCode() {
        int result = (int) (timeStamp ^ (timeStamp >>> 32));
        result = 31 * result + (int) (seqNum ^ (seqNum >>> 32));
        result = 31 * result + (ticker != null ? ticker.hashCode() : 0);
        return result;
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
