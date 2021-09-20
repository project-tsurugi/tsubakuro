package com.nautilus_technologies.tsubakuro.low.tpcc;

public class Profile {
    public class Counter {
	long newOrder;
	long payment;
	long delivery;
	long orderStatus;
	long stockLevel;

	public Counter() {
	}
	void initialize() {
	    newOrder = 0;
	    payment = 0;
	    delivery = 0;
	    orderStatus = 0;
	    stockLevel = 0;
	}
	void add(Counter counter) {
	    newOrder += counter.newOrder;
	    payment += counter.payment;
	    delivery += counter.delivery;
	    orderStatus += counter.orderStatus;
	    stockLevel += counter.stockLevel;
	}
    }

    public long warehouses;
    public long index;
    public Counter invocation;
    public Counter completion;
    public Counter retry;
    public Counter error;
    public long newOrderIntentionalRollback;

    public Profile() {
	invocation.initialize();
	completion.initialize();
	retry.initialize();
	error.initialize();
	newOrderIntentionalRollback = 0;
    }
    public void add(Profile profile) {
	invocation.add(profile.invocation);
	completion.add(profile.completion);
	retry.add(profile.retry);
	error.add(profile.error);
	newOrderIntentionalRollback += profile.newOrderIntentionalRollback;
    }
}
