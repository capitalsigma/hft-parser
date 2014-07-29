package com.hftparser.readers;

import ch.systemsx.cisd.hdf5.CompoundElement;

import java.util.Arrays;

// maybe this should go in with the writers
public class DataPoint {
    private int[][] buy;
    private int[][] sell;
    private int timeStamp;
    private long seqNum;
    private String ticker;


	public DataPoint(String _ticker,
                     int[][] _buy,
                     int[][] _sell,
                     int _timeStamp,
                     long _seqNum) {
		ticker = _ticker;
		buy = _buy;
        sell = _sell;
		timeStamp = _timeStamp;
		seqNum = _seqNum;
	}

	public void print() {
        System.out.println("Information for hashcode: " + this.hashCode());
		System.out.printf("my tick: %s\n", ticker);
		System.out.printf("my seq: %d\n", seqNum);
		System.out.printf("my ts: %d\n", timeStamp);

		System.out.printf("my buy: %s\n", Arrays.deepToString(buy));
        System.out.printf("my sell: %s\n", Arrays.deepToString(sell));
	}


	public boolean equals(DataPoint other) {
		if(other == null) {
			return false;
		}

        boolean res = other.ticker.equals(ticker) &&
                (other.seqNum == seqNum) &&
                (other.timeStamp == timeStamp) &&
                Arrays.deepEquals(other.buy, buy) &&
                Arrays.deepEquals(other.sell, sell);

        System.out.println("Testing pair:");

        print();
        other.print();

        System.out.println("Equal?" + res);

		return res;
	}
}
