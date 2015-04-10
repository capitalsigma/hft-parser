package com.hftparser.writers;

/**
 * Created by patrick on 8/7/14.
 */
public abstract class ElementCache<T> {
    protected final T[] cache;
    protected int currentCacheOffset;

    protected ElementCache(int maxSize) {
        //noinspection unchecked
        cache = (T[]) new Object[maxSize];
    }

    public void appendElement(T element) {
        cache[currentCacheOffset++] = element;
    }

    public boolean isFull() {
        return currentCacheOffset == cache.length;
    }

    public T[] getElements() {
        return fixUp();
    }

    public int getCurrentCacheOffset() {
        return currentCacheOffset;
    }

    public void resetCurrentCacheOffset() {
        currentCacheOffset = 0;
    }


    public abstract void setEmptyElement(T element);

    protected abstract T[] fixUp();
}

