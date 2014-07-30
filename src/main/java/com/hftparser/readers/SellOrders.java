package com.hftparser.readers;

import java.util.TreeSet;

class SellOrders extends MarketOrderCollection {
	// ascending sort for prices, following utils.py
	public SellOrders(){
		super();
		sortedKeys = new TreeSet<>();
	}
}
