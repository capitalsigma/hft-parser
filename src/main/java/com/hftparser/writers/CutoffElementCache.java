package com.hftparser.writers;

import java.util.Arrays;

/**
 * Created by patrick on 8/7/14.
 */
public class CutoffElementCache<T> extends ElementCache<T> {
    public CutoffElementCache(int maxSize) {
        super(maxSize);
    }

    @Override
    public void setEmptyElement(T element) {
    }

    @Override
    protected T[] fixUp() {
        if (currentCacheOffset == cache.length) {
            return cache;
        } else {
            return Arrays.copyOfRange(cache, 0, currentCacheOffset);
        }
    }
}
