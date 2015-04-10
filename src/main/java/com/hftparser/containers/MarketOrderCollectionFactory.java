package com.hftparser.containers;

import com.hftparser.config.MarketOrderCollectionConfig;

/**
 * Created by patrick on 8/4/14.
 */
public class MarketOrderCollectionFactory {
    private int startCapacity;
    private int maxKeyCount;
    private boolean doCaching;


    public MarketOrderCollectionFactory(int startCapacity, int maxKeyCount, boolean doCaching) {
        this.startCapacity = startCapacity;
        this.maxKeyCount = maxKeyCount;
        this.doCaching = doCaching;
    }

    public MarketOrderCollectionFactory(MarketOrderCollectionConfig config) {
        this(config.getStartCapacity(), config.getMaxKeyCount(), config.isCaching());
    }

    public MarketOrderCollectionFactory() {
        this(MarketOrderCollectionConfig.getDefault());
    }

    public void setStartCapacity(int startCapacity) {
        this.startCapacity = startCapacity;
    }

    public void setMaxKeyCount(int maxKeyCount) {
        this.maxKeyCount = maxKeyCount;
    }

    public void setDoCaching(boolean doCaching) {
        this.doCaching = doCaching;
    }

    private MarketOrderCollection buildOrders(MarketOrderCollection toRet) {
        if (doCaching) {
            return new MarketOrderCollectionCache(toRet);
        } else {
            return toRet;
        }
    }

    public MarketOrderCollection buildBuy() {
        return buildOrders(new BuyOrders(startCapacity, maxKeyCount));
    }

    public MarketOrderCollection buildSell() {
        return buildOrders(new SellOrders(startCapacity, maxKeyCount));
    }
}
