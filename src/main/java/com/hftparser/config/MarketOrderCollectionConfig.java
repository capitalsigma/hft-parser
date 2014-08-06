package com.hftparser.config;

import org.json.JSONObject;

/**
 * Created by patrick on 8/4/14.
 */
public class MarketOrderCollectionConfig {
    private final int startCapacity;
    private final int maxKeyCount;
    private final boolean caching;

    public int getStartCapacity() {
        return startCapacity;
    }

    public int getMaxKeyCount() {
        return maxKeyCount;
    }

    public boolean isCaching() {
        return caching;
    }

    public MarketOrderCollectionConfig(int startCapacity, int maxKeyCount, boolean caching) {
        this.startCapacity = startCapacity;
        this.maxKeyCount = maxKeyCount;
        this.caching = caching;
    }

    public MarketOrderCollectionConfig(JSONObject json) {
        startCapacity = json.getInt("start_capacity");
        maxKeyCount = json.getInt("max_key_count");
        caching = json.getBoolean("caching");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MarketOrderCollectionConfig that = (MarketOrderCollectionConfig) o;

        if (caching != that.caching) {
            return false;
        }
        if (maxKeyCount != that.maxKeyCount) {
            return false;
        }
        if (startCapacity != that.startCapacity) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = startCapacity;
        result = 31 * result + maxKeyCount;
        result = 31 * result + (caching ? 1 : 0);
        return result;
    }

    public static MarketOrderCollectionConfig getDefault() {
        return new MarketOrderCollectionConfig(100, 10, false);
    }
}
