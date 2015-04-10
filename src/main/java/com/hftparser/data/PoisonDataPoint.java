package com.hftparser.data;

/**
 * Created by patrick on 4/5/15. The purpose of this class is to trigger that a particular ticker be purged from the
 * HDF5 file. This is a pretty ugly hack, but it's tricky to pass around data across all of the threads that are
 * running, and this gets the job done with minimal hassle.
 */
public class PoisonDataPoint implements DataPoint {
    private final String ticker;

    public PoisonDataPoint(String ticker) {
        this.ticker = ticker;
    }

    @Override
    public String getTicker() {
        return ticker;
    }

    @Override
    public void print() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WritableDataPoint getWritable() {
        throw new PoisonDataPointException(ticker);
    }

    @Override
    public long getTimeStamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getBuy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getSell() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return ticker.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PoisonDataPoint that = (PoisonDataPoint) o;

        return ticker.equals(that.ticker);
    }
}
