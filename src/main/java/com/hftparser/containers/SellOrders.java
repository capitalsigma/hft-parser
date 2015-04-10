package com.hftparser.containers;

import java.util.TreeSet;

public class SellOrders extends MarketOrderCollection {
    // ascending sort for prices, following utils.py
    public SellOrders() {
        this(100, 10);
    }

    SellOrders(int startCapacity, int maxKeyCount) {
        super(startCapacity, maxKeyCount);
        sortedKeys = new TreeSet<>();
    }
}
