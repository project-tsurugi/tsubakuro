package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.util.concurrent.atomic.AtomicBoolean;

public class DeferredHelper {
    AtomicBoolean[] doingDelivery;

    public DeferredHelper(long n) {
	this.doingDelivery = new AtomicBoolean[(int) n];
	for (int i = 0; i < n; i++) {
	    doingDelivery[i] = new AtomicBoolean(false);
	}
    }

    public boolean getAndSet(int i, boolean b) {
	return doingDelivery[i].getAndSet(b);
    }

    public void set(int i, boolean b) {
	doingDelivery[i].set(b);
    }
}
