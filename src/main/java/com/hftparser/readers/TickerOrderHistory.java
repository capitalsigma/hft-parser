package com.hftparser.readers;

import java.util.HashMap;
import java.util.Map;

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
        System.out.println("nextSeqNum = [" + nextSeqNum + "]");
        System.out.println("lastSeqNum = " + lastSeqNum);
        if (nextSeqNum <= lastSeqNum) {
            return false;
        } else {
            lastSeqNum = nextSeqNum;
            return true;
        }
    }

    public int size() {
        return outstandingOrders.size();
    }

    public Order put(Long refNum, Order order) {
        return outstandingOrders.put(refNum, order);
    }

    public Order remove(Long refNum) {
        return outstandingOrders.remove(refNum);
    }

    public Order get(Long refNum) {
        return outstandingOrders.get(refNum);
    }
}
