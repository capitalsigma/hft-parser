package com.hftparser.readers;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by patrick on 8/4/14.
 */
public class MarketOrderCollectionCache extends MarketOrderCollection {
    private MarketOrderCollection decoratedCollection;
    long[][] cache;
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
    public long[][] topN(int topN) {
        return decoratedCollection.topN(topN);
    }

    @Override
    public long[][] topN() {
        if (isDirty()) {
            cache = decoratedCollection.topN();
            dirty = false;
//            System.out.println("Setting dirty false");
        }

        return cache;
    }


    @Override
    public Long put(Long price, Long quantity) {
//        System.out.println("decoratedCollection.sortedKeys.headSet(price).size(): " +
//                                   decoratedCollection.sortedKeys.headSet(price).size());

        if (decoratedCollection.sortedKeys.headSet(price).size() < decoratedCollection.maxKeyCount) {
            dirty = true;
        }

        return decoratedCollection.put(price, quantity);
    }

    @Override
    public int size() {
        return decoratedCollection.size();
    }

    @Override
    public boolean isEmpty() {
        return decoratedCollection.isEmpty();
    }

    @Override
    public Long get(Object key) {
        return decoratedCollection.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return decoratedCollection.containsKey(key);
    }

    @Override
    public void putAll(Map<? extends Long, ? extends Long> m) {
        decoratedCollection.putAll(m);
    }

    @Override
    public Long remove(Object key) {
        return decoratedCollection.remove(key);
    }

    @Override
    public void clear() {
        decoratedCollection.clear();
    }

    @Override
    public boolean containsValue(Object value) {
        return decoratedCollection.containsValue(value);
    }

    @Override
    public Object clone() {
        return decoratedCollection.clone();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Set<Long> keySet() {
        return decoratedCollection.keySet();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Collection<Long> values() {
        return decoratedCollection.values();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Set<Map.Entry<Long, Long>> entrySet() {
        return decoratedCollection.entrySet();
    }
}
