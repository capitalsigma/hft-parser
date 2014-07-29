package com.hftparser.readers;

import ch.systemsx.cisd.hdf5.CompoundElement;

/**
 * Created by patrick on 7/29/14.
 */
public class WritableDataPoint {
    @CompoundElement(memberName = "bid", dimensions = {10, 2})
    private int[][] buy;

    @CompoundElement(memberName =  "ask", dimensions = {10, 2})
    private int[][] sell;

    @CompoundElement(memberName = "timestamp")
    private int timeStamp;

    @CompoundElement(memberName = "seqnum")
    private long seqNum;

    public WritableDataPoint(int[][] buy, int[][] sell, int timeStamp, long seqNum) {
        this.buy = buy;
        this.sell = sell;
        this.timeStamp = timeStamp;
        this.seqNum = seqNum;
    }

    //    needed for the HDF5 business
    public WritableDataPoint(){

    }
}
