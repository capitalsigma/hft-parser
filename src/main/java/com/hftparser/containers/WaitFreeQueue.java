package com.hftparser.containers;

// This is a parallel, wait-free queue that *only* works for a single
// producer, single consumer. It is fast, but IT WILL BREAK IF YOU PUT
// MORE THAN ONE THREAD ON EITHER SIDE. For more info, look up
// "Lamport Queue."

import java.util.concurrent.atomic.AtomicLong;

//TODO: remove this tracking stuff after profiling
// Source: The Art of Multiprocessor Programming, p. 69
public class WaitFreeQueue<T> {
	private volatile int head = 0;
    private volatile int tail = 0;
	private final T[] items;
	volatile public boolean acceptingOrders;
    private AtomicLong fullHits ;
    private AtomicLong emptyHits;


	public WaitFreeQueue(int capacity) {
        //noinspection unchecked
        items = (T[])new Object[capacity];
		head = 0;
		tail = 0;
		acceptingOrders = true;

        fullHits = new AtomicLong();
        emptyHits = new AtomicLong();
    }

	public boolean enq(T x) {
		if(tail - head == items.length) {
			// throw new FullException();
            fullHits.incrementAndGet();
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
        System.out.printf("Full hits: %s\nEmpty hits: %s", fullHits.toString(), emptyHits.toString());
    }
}
