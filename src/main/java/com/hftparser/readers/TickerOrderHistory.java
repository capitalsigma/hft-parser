package com.hftparser.readers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The TickerOrderHistory contains all of the current outstanding add orders (i.e. those orders that have not yet been
 * deleted) indexed on refNum, in addition to the seqnum of the last order added to this ticker.
 */
public class TickerOrderHistory {
    private final Map<Long, Order> outstandingOrders;
    private long lastSeqNum = -1;

    public TickerOrderHistory(int initialSize) {
        outstandingOrders = new HashMap<>(initialSize);
    }

    // Returns true if the next seqnum is valid and updates the current seqnum as a side effect, otherwise, returns
    // faslse
    public boolean updateSeqNum(long nextSeqNum) {
        if (nextSeqNum >= lastSeqNum) {
            return false;
        } else {
            lastSeqNum = nextSeqNum;
            return true;
        }
    }

    public int size() {
        return outstandingOrders.size();
    }

    public Order put(Long aLong, Order order) {
        return outstandingOrders.put(aLong, order);
    }

    public Set<Long> keySet() {
        return outstandingOrders.keySet();
    }

    public Set<Map.Entry<Long, Order>> entrySet() {
        return outstandingOrders.entrySet();
    }

    public boolean containsKey(Object o) {
        return outstandingOrders.containsKey(o);
    }

    public Order remove(Object o) {
        return outstandingOrders.remove(o);
    }

    public void putAll(Map<? extends Long, ? extends Order> map) {
        outstandingOrders.putAll(map);
    }

    public boolean isEmpty() {
        return outstandingOrders.isEmpty();
    }

    public void clear() {
        outstandingOrders.clear();
    }

    public boolean containsValue(Object o) {
        return outstandingOrders.containsValue(o);
    }

    public Collection<Order> values() {
        return outstandingOrders.values();
    }

    public Order get(Object o) {
        return outstandingOrders.get(o);
    }
}
