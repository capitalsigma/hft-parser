package com.hftparser.readers;

import java.util.HashMap;

class KeyError extends RuntimeException {
    KeyError(String message) {
        super(message);
    }
}

/**
 * Modifies the default HashMap implementation to throw an exception when a key isn't present
 */
public class NonnullHashMap<K, V> extends HashMap<K, V> {
//    Need this to be a runtime exception so we don't break the contract of Map


    public NonnullHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public NonnullHashMap() {
        super();
    }

    @Override
    public V get(Object key) {
        V maybeVal = super.get(key);
        if (maybeVal == null) {
            throw new KeyError("Key not present: " + key.toString());
        } else {
            return maybeVal;
        }
    }
}
