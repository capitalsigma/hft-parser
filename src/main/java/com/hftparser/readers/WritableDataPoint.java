package com.hftparser.readers;

import ch.systemsx.cisd.hdf5.CompoundElement;

import java.util.Arrays;

/**
 * Created by patrick on 7/29/14.
 */
public class WritableDataPoint {

//    NOTE: this is ugly, but this way it doesn't get written out to the output.
//    TODO: refactor this as an inner class of something to avoid this
//    private final int LEVELS = ArcaParser.LEVELS;
//NOTE: 172 bytes per record

    @CompoundElement(memberName = "bid", dimensions = {ArcaParser.LEVELS, 2})
    protected long[][] buy;

    @CompoundElement(memberName =  "ask", dimensions = {ArcaParser.LEVELS, 2})
    protected long[][] sell;

    @CompoundElement(memberName = "timestamp")
    private long timeStamp;

    @CompoundElement(memberName = "seqnum")
    protected long seqNum;

    public WritableDataPoint(long[][] buy, long[][] sell, long timeStamp, long seqNum) {
        this.buy = padArray(buy);
        this.sell = padArray(sell);
        this.timeStamp = timeStamp;
        this.seqNum = seqNum;
    }

    private long[][] padArray(long[][] toPad){
        if (toPad.length != ArcaParser.LEVELS) {
            return buildNew(toPad);
        }
        return toPad;
    }

    private long[][] buildNew(long[][] toPad){
        long[][] ret = new long[ArcaParser.LEVELS][2];

        for(int i = 0; i < ArcaParser.LEVELS; i++){
//            System.out.println("Checking for line:" + i + ", in.len = " + toPad.length);
            if((toPad.length <= i) || (toPad[i] == null)) {
                ret[i] = new long[] {0, 0};
            } else {
                ret[i] = toPad[i];
            }
        }
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WritableDataPoint that = (WritableDataPoint) o;

        return equals(that);

//        if (seqNum != that.seqNum) {
//            return false;
//        }
//        if (timeStamp != that.timeStamp) {
//            return false;
//        }
//
//        return true;
    }

    public boolean equals(WritableDataPoint other){
        return (other != null) &&
                other.timeStamp == timeStamp &&
                other.seqNum == seqNum &&
                Arrays.deepEquals(other.buy, buy) &&
                Arrays.deepEquals(other.sell, sell);
    }

    //    needed for the HDF5 business
    public WritableDataPoint(){

    }

    @Override
    public String toString() {
        return "WritableDataPoint{" +
                "buy=" + Arrays.deepToString(buy) +
                ", sell=" + Arrays.deepToString(sell) +
                ", timeStamp=" + timeStamp +
                ", seqNum=" + seqNum +
                '}';
    }
}
