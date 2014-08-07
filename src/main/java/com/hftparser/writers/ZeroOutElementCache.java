package com.hftparser.writers;

/**
 * Created by patrick on 8/7/14.
 */
public class ZeroOutElementCache<T> extends ElementCache<T> {
    T emptyElement;

    public ZeroOutElementCache(int maxSize) {
        super(maxSize);
    }

    @Override
    public void setEmptyElement(T element) {
        emptyElement = element;
    }

    @Override
    protected T[] fixUp() {
        for (int i = currentCacheOffset; i < cache.length; i++) {
            cache[i] = emptyElement;
        }

        return cache;
    }
}
