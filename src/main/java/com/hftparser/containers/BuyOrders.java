package com.hftparser.containers;

import java.util.Collections;
import java.util.TreeSet;

public class BuyOrders extends MarketOrderCollection {
    // descending sort for prices, following utils.py
    public BuyOrders(int startCapacity, int topN) {
        super(startCapacity, topN);
        sortedKeys = new TreeSet<>(Collections.reverseOrder());
    }

    public BuyOrders() {
        this(100, 10);
    }
}
