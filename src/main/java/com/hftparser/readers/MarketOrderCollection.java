package com.hftparser.readers;

import java.util.HashMap;
import java.util.TreeSet;


abstract class MarketOrderCollection extends HashMap<Long,Long> {
	protected TreeSet<Long> sortedKeys;
    protected int keyCount;
    protected int maxKeyCount;

	// In case we want to do something special later
    MarketOrderCollection() {
		// default size (can't define as a property)
//        TODO: make this bigger
		this(10, 0);
	}

	MarketOrderCollection(int startCapacity, int maxKeyCount) {
		super(startCapacity);
        keyCount = 0;
        this.maxKeyCount = maxKeyCount;
//        this is always true

    }

//    this is up to the decorator to implement -- in the default, we treat it as if it's always true
    public boolean isDirty() {
        return true;
    }

	public long[][] topN(int topN) {
		// can't zip lists in Java

		long[][] ret = new long[Math.min(keyCount, topN)][2];

		// for(Integer i = 0; i < topN; i++) {
		int i = 0;
		for (Long price : sortedKeys){

			ret[i][0] = price;
			ret[i][1] = get(price);

			// System.out.println("Set for price: " + price + " with qty " +
			// 				   ret[i][1]);

			if(++i == topN) {
				break;
			}
		}

		return ret;
	}

    public long[][] topN() {
        return topN(maxKeyCount);
    }

	@Override
	public Long put(Long price, Long quantity) {
		Long ret;

		ret = super.put(price, quantity);

		// System.out.printf("Setting for price: %d, qty: %d\n", price, quantity);
		// System.out.println("About to update sortedKeys: " +
		// 				   Arrays.toString(sortedKeys.toArray()));


		if(quantity == 0) {
 			// System.out.println("Removing.");
			sortedKeys.remove(price);
            keyCount--;
        } else {
			// System.out.println("Adding.");
			sortedKeys.add(price);
            keyCount++;
        }

		// System.out.println("Updated sortedKeys: " +
		// 				   Arrays.toString(sortedKeys.toArray()));


		return ret;
	}

}
