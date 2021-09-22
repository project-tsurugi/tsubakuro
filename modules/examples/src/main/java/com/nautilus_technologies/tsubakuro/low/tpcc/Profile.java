package com.nautilus_technologies.tsubakuro.low.tpcc;

public class Profile {
    public class Counter {
	long newOrder;
	long payment;
	long delivery;
	long orderStatus;
	long stockLevel;

	public Counter() {
	    this.newOrder = 0;
	    this.payment = 0;
	    this.delivery = 0;
	    this.orderStatus = 0;
	    this.stockLevel = 0;
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
    public long elapsed;
    public long count;

    public Profile() {
	invocation = new Counter();
	completion = new Counter();
	retry = new Counter();
	error = new Counter();
	newOrderIntentionalRollback = 0;
	count = 0;
    }
    public void add(Profile profile) {
	invocation.add(profile.invocation);
	completion.add(profile.completion);
	retry.add(profile.retry);
	error.add(profile.error);
	newOrderIntentionalRollback += profile.newOrderIntentionalRollback;
	elapsed += profile.elapsed;
	count++;
    }
    public void print() {
	System.out.println(elapsed);
	System.out.println(invocation.newOrder + ":" + completion.newOrder + ":" + retry.newOrder + ":" + newOrderIntentionalRollback);
	System.out.println(invocation.payment + ":" + completion.payment + ":" + retry.payment);
	System.out.println(invocation.delivery + ":" + completion.delivery + ":" + retry.delivery);
	System.out.println(invocation.orderStatus + ":" + completion.orderStatus + ":" + retry.orderStatus);
	System.out.println(invocation.stockLevel + ":" + completion.stockLevel + ":" + retry.stockLevel);
    }
}
