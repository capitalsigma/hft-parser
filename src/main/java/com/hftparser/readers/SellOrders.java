package com.hftparser.readers;

import java.util.TreeSet;

class SellOrders extends MarketOrderCollection {
    // ascending sort for prices, following utils.py
    public SellOrders() {
        this(100, 10);
    }

    SellOrders(int startCapacity, int maxKeyCount) {
        super(startCapacity, maxKeyCount);
        sortedKeys = new TreeSet<>();
    }
}
