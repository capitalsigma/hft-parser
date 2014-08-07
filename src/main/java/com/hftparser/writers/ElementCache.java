package com.hftparser.writers;

/**
 * Created by patrick on 8/7/14.
 */
abstract public class ElementCache<T> {
    protected T[] cache;
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


    abstract public void setEmptyElement(T element);
    abstract protected T[] fixUp();
}

