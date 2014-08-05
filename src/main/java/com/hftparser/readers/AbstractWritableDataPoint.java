//package com.hftparser.readers;
//
//import ch.systemsx.cisd.hdf5.CompoundElement;
//
//import java.util.Arrays;
//
///**
// * Created by patrick on 8/4/14.
// */
//public class AbstractWritableDataPoint {
//
//
//
//
//    public AbstractWritableDataPoint() {
//
//    }
//
//    public AbstractWritableDataPoint(int[][] buy, int[][] sell, long seqNum) {
//        this.buy = buy;
//        this.sell = sell;
//        this.seqNum = seqNum;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) {
//            return true;
//        }
//        if (o == null || getClass() != o.getClass()) {
//            return false;
//        }
//
//        AbstractWritableDataPoint that = (AbstractWritableDataPoint) o;
//
//        if (seqNum != that.seqNum) {
//            return false;
//        }
//
//        return Arrays.deepEquals(buy, that.buy) && Arrays.deepEquals(sell, that.sell);
//    }
//
//    @Override
//    public int hashCode() {
//        return (int) (seqNum ^ (seqNum >>> 32));
//    }
//}
