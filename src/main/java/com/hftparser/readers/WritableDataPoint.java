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

    @CompoundElement(memberName = "bid", dimensions = {ArcaParser.LEVELS, 2})
    private int[][] buy;

    @CompoundElement(memberName =  "ask", dimensions = {ArcaParser.LEVELS, 2})
    private int[][] sell;

    @CompoundElement(memberName = "timestamp")
    private int timeStamp;

    @CompoundElement(memberName = "seqnum")
    private long seqNum;

    public WritableDataPoint(int[][] buy, int[][] sell, int timeStamp, long seqNum) {
        this.buy = padArray(buy);
        this.sell = padArray(sell);
        this.timeStamp = timeStamp;
        this.seqNum = seqNum;
    }

    private int[][] padArray(int[][] toPad){
        if (toPad.length != ArcaParser.LEVELS) {
            return buildNew(toPad);
        }
        return toPad;
    }

    private int[][] buildNew(int[][] toPad){
        int[][] ret = new int[ArcaParser.LEVELS][2];

        for(int i = 0; i < ArcaParser.LEVELS; i++){
//            System.out.println("Checking for line:" + i + ", in.len = " + toPad.length);
            if((toPad.length <= i) || (toPad[i] == null)) {
                ret[i] = new int[] {0, 0};
            } else {
                ret[i] = toPad[i];
            }
        }
        return ret;
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
}
