package com.hftparser.data;

/**
 * Created by patrick on 4/5/15.
 */
public class PoisonDataPointException extends RuntimeException {
    String poisonTicker;

    public PoisonDataPointException(String poisonTicker) {
        this.poisonTicker = poisonTicker;
    }

    public String getPoisonTicker() {
        return poisonTicker;
    }
}
