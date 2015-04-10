package com.hftparser.data;

/**
 * Created by patrick on 4/5/15.
 */
public interface DataPoint {
    String getTicker();

    void print();

    @Override
    int hashCode();

    @Override
    boolean equals(Object o);

    @Override
    String toString();

    WritableDataPoint getWritable();

    long getTimeStamp();

    long[][] getBuy();

    long[][] getSell();
}
