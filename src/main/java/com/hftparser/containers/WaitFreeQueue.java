package com.hftparser.containers;

// This is a parallel, wait-free queue that *only* works for a single
// producer, single consumer. It is fast, but IT WILL BREAK IF YOU PUT
// MORE THAN ONE THREAD ON EITHER SIDE. For more info, look up
// "Lamport Queue."

import java.util.concurrent.atomic.AtomicLong;

//TODO: remove this tracking stuff after profiling
// Source: The Art of Multiprocessor Programming, p. 69
public class WaitFreeQueue<T> {
    private final Backoffable inBackoff;
    private final Backoffable outBackoff;
    private final T[] items;
    private final AtomicLong fullHits;
    private final AtomicLong emptyHits;
    public volatile boolean acceptingOrders;
    private volatile int head = 0;
    private volatile int tail = 0;


    public WaitFreeQueue(int capacity, Backoffable inBackoff, Backoffable outBackoff) {
        //noinspection unchecked
        items = (T[]) new Object[capacity];
        head = 0;
        tail = 0;
        acceptingOrders = true;

        fullHits = new AtomicLong();
        emptyHits = new AtomicLong();

        this.inBackoff = inBackoff;
        this.outBackoff = outBackoff;
    }

    public WaitFreeQueue(int capacity) {
        this(capacity, new NoOpBackoff(), new NoOpBackoff());
    }

    public boolean enq(T x) {
        if (tail - head == items.length) {
            // throw new FullException();
            fullHits.incrementAndGet();
            inBackoff.backoff();
            return false;
        } else {
            items[tail % items.length] = x;
            tail++;
            return true;
        }
    }

    public T deq() {
        if (isEmpty()) {
            // throw new EmptyException();
            emptyHits.incrementAndGet();
            outBackoff.backoff();
            return null;
        } else {
            T x = items[head % items.length];
            head++;
            return x;
        }
    }

    public boolean isEmpty() {
        return tail - head == 0;
    }

    public void printUsage() {
        System.out.printf("Full hits: %s\nEmpty hits: %s\n", fullHits.toString(), emptyHits.toString());
    }
}
