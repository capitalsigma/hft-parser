package com.hftparser.readers;

import ch.systemsx.cisd.hdf5.CompoundElement;

import java.util.Arrays;

/**
 * Created by patrick on 8/4/14.
 */
public class PythonWritableDataPoint extends AbstractWritableDataPoint {

    @CompoundElement(memberName = "timestamp_s", dimensions = {16})
    protected String timestamp_s;


    @CompoundElement(memberName = "timestamp")
    private long timeStamp;

    PythonWritableDataPoint() {
//        need default constructor for library
    }

    public PythonWritableDataPoint(int[][] buy, int[][] sell, long seqNum, String timestamp_s, long timeStamp) {
        super(buy, sell, seqNum);
        this.timestamp_s = timestamp_s;
        this.timeStamp = timeStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        PythonWritableDataPoint that = (PythonWritableDataPoint) o;

        if (timeStamp != that.timeStamp) {
            return false;
        }
        if (timestamp_s != null ? !timestamp_s.equals(that.timestamp_s) : that.timestamp_s != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = timestamp_s != null ? timestamp_s.hashCode() : 0;
        result = 31 * result + (int) (timeStamp ^ (timeStamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "PythonWritableDataPoint{" +
                "timestamp_s='" + timestamp_s + '\'' +
                ", timeStamp=" + timeStamp +
                ", seqnum=" + seqNum +
                ", buy=" + Arrays.deepToString(buy) +
                ", sell=" + Arrays.deepToString(sell) +
                '}';
    }
}

