package com.hftparser.writers;

/**
 * Created by patrick on 8/7/14.
 */
public class ElementCacheFactory<T> {
    private int maxSize;
    private boolean isCutoff;
    private T emptyElement;

    public ElementCacheFactory(int maxSize, boolean isCutoff) {
        this.maxSize = maxSize;
        this.isCutoff = isCutoff;
    }

    public ElementCacheFactory() {
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public boolean isCutoff() {
        return isCutoff;
    }

    public void setCutoff(boolean isCutoff) {
        this.isCutoff = isCutoff;
    }

    public T getEmptyElement() {
        return emptyElement;
    }

    public void setEmptyElement(T emptyElement) {
        this.emptyElement = emptyElement;
    }

    public CutoffElementCache<T> getCutoff() {
        return new CutoffElementCache<>(maxSize);
    }

    public ZeroOutElementCache<T> getZeroOut() {
        ZeroOutElementCache<T> toRet = new ZeroOutElementCache<>(maxSize);
        toRet.setEmptyElement(emptyElement);
        return toRet;
    }

    public ElementCache<T> getCache() {
        if (isCutoff) {
            return getCutoff();
        } else {
            return getZeroOut();
        }
    }

}
