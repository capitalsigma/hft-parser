package com.hftparser.data;

/**
 * Created by patrick on 4/5/15.
 */
public interface DataPoint {
    String getTicker();

    void print();

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();

    WritableDataPoint getWritable();

    @Override
    String toString();

    long getTimeStamp();

    long[][] getBuy();

    long[][] getSell();
}
