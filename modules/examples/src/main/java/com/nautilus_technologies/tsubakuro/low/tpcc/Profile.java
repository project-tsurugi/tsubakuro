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
    public Counter retryOnStatement;
    public Counter retryOnCommit;
    public Counter error;
    public long newOrderIntentionalRollback;
    public long elapsed;
    public long count;

    public Profile() {
	invocation = new Counter();
	completion = new Counter();
	retryOnStatement = new Counter();
	retryOnCommit = new Counter();
	error = new Counter();
	newOrderIntentionalRollback = 0;
	count = 0;
    }
    public void add(Profile profile) {
	invocation.add(profile.invocation);
	completion.add(profile.completion);
	retryOnStatement.add(profile.retryOnStatement);
	retryOnCommit.add(profile.retryOnCommit);
	error.add(profile.error);
	newOrderIntentionalRollback += profile.newOrderIntentionalRollback;
	elapsed += profile.elapsed;
	count++;
    }
    public void print(int threads) {
	System.out.println("duration(mS): " + elapsed);
	System.out.println("     tx type: invocation:completion(:intentional rollback) - retry on statement:retry on commit");
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.println("   new order: " + invocation.newOrder + ":" + completion.newOrder + ":" + newOrderIntentionalRollback + " - " + retryOnStatement.newOrder + ":" + retryOnCommit.newOrder);
	System.out.println("     payment: " + invocation.payment + ":" + completion.payment + " - " + retryOnStatement.payment + ":" + retryOnCommit.payment);
	System.out.println("    delivery: " + invocation.delivery + ":" + completion.delivery + " - " + retryOnStatement.delivery + ":" + retryOnCommit.delivery);
	System.out.println("order status: " + invocation.orderStatus + ":" + completion.orderStatus + " - " + retryOnStatement.orderStatus + ":" + retryOnCommit.orderStatus);
	System.out.println(" stock level: " + invocation.stockLevel + ":" + completion.stockLevel + " - " + retryOnStatement.stockLevel + ":" + retryOnCommit.stockLevel);
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.printf("#NoTPM %.2f\n", ((double) completion.newOrder * 60.0 * 1000.0) / ((double) elapsed / (double) threads));
    }
}
