package com.hftparser.readers;

import java.util.Collections;
import java.util.TreeSet;

class BuyOrders extends MarketOrderCollection {
	// descending sort for prices, following utils.py
    private BuyOrders(int startCapacity){
		super(startCapacity);
		sortedKeys = new TreeSet<Integer>(Collections.reverseOrder());
	}

	public BuyOrders() {
		this(10);
	}
}
