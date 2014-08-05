package com.hftparser.readers;

/**
 * Created by patrick on 8/4/14.
 */
public class MarketOrderCollectionCache extends MarketOrderCollection {
    public MarketOrderCollection decoratedCollection;
    int[][] cache;
    private boolean dirty;

    public MarketOrderCollectionCache(MarketOrderCollection decoratedCollection) {
        this.decoratedCollection = decoratedCollection;
        dirty = true;
    }

    public boolean isDirty() {
        return dirty;
    }

    //    marking as deprecated because we don't want to use this -- it messes up our cache
    @Deprecated
    public int[][] topN(int topN) {
        return decoratedCollection.topN(topN);
    }

    @Override
    public int[][] topN() {
        if (isDirty()) {
            cache = decoratedCollection.topN();
            dirty = false;
//            System.out.println("Setting dirty false");
        }

        return cache;
    }

    @Override
    public Integer put(Integer price, Integer quantity) {
//        System.out.println("decoratedCollection.sortedKeys.headSet(price).size(): " +
//                                   decoratedCollection.sortedKeys.headSet(price).size());

        if (decoratedCollection.sortedKeys.headSet(price).size() < decoratedCollection.maxKeyCount) {
            dirty = true;
        }

        return decoratedCollection.put(price, quantity);
    }

    @Override
    public String toString() {
        return decoratedCollection.toString();
    }
}
