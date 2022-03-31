package com.nautilus_technologies.tsubakuro.low.tpcc;

public class Profile {
    static class Counter {
	long newOrder;
	long payment;
	long orderStatus;
	long delivery;
	long stockLevel;

	Counter() {
	    this.newOrder = 0;
	    this.payment = 0;
	    this.orderStatus = 0;
	    this.delivery = 0;
	    this.stockLevel = 0;
	}
	void add(Counter counter) {
	    newOrder += counter.newOrder;
	    payment += counter.payment;
	    orderStatus += counter.orderStatus;
	    delivery += counter.delivery;
	    stockLevel += counter.stockLevel;
	}
    }

    public long warehouses;
    public long threads;
    public long index;
    public boolean fixThreadMapping;
    public Counter time;
    public Counter invocation;
    public Counter completion;
    public Counter retryOnStatement;
    public Counter retryOnCommit;
    public Counter error;
    public Counter districtTable;
    public Counter warehouseTable;
    public Counter ordersTable;
    public Counter customerTable;
    public Counter stockTable;
    public long newOrderIntentionalRollback;
    public long elapsed;
    public long count;
    public long inconsistentIndexCount;  // for temporary use

    public Profile() {
	time = new Counter();
	invocation = new Counter();
	completion = new Counter();
	retryOnStatement = new Counter();
	retryOnCommit = new Counter();
	error = new Counter();
	districtTable = new Counter();
	warehouseTable = new Counter();
	ordersTable = new Counter();
	customerTable = new Counter();
	stockTable = new Counter();
	newOrderIntentionalRollback = 0;
	count = 0;
	inconsistentIndexCount = 0;  // for temporary use
    }
    public void add(Profile profile) {
	time.add(profile.time);
	invocation.add(profile.invocation);
	completion.add(profile.completion);
	retryOnStatement.add(profile.retryOnStatement);
	retryOnCommit.add(profile.retryOnCommit);
	error.add(profile.error);
	districtTable.add(profile.districtTable);
	warehouseTable.add(profile.warehouseTable);
	ordersTable.add(profile.ordersTable);
	customerTable.add(profile.customerTable);
	stockTable.add(profile.stockTable);
	newOrderIntentionalRollback += profile.newOrderIntentionalRollback;
	elapsed += profile.elapsed;
	count++;
	inconsistentIndexCount += profile.inconsistentIndexCount;  // for temporary use
    }
    long ns2us(long t) {
	return (t + 500) / 1000;
    }
    long div(long a, long b) {
	if (b == 0) {
	    return a;
	}
	return a / b;
    }
    public void print(int n) {
	if (inconsistentIndexCount > 0) {  // for temporary use
	    System.out.printf("retry due to inconsistent_index: %d times%n%n", inconsistentIndexCount);
	}
	System.out.printf("duration(mS): %d%n", elapsed / count);
	System.out.println("===============================================================================================");
	System.out.printf("   new order: %12d / %8d = %6d (us)%n", ns2us(time.newOrder), completion.newOrder + newOrderIntentionalRollback, ns2us(div(time.newOrder , (completion.newOrder + newOrderIntentionalRollback))));
	System.out.printf("     payment: %12d / %8d = %6d (us)%n", ns2us(time.payment), completion.payment, ns2us(div(time.payment, completion.payment)));
	System.out.printf("order status: %12d / %8d = %6d (us)%n", ns2us(time.orderStatus), completion.orderStatus, ns2us(div(time.orderStatus, completion.orderStatus)));
	System.out.printf("    delivery: %12d / %8d = %6d (us)%n", ns2us(time.delivery), completion.delivery, ns2us(div(time.delivery, completion.delivery)));
	System.out.printf(" stock level: %12d / %8d = %6d (us)%n", ns2us(time.stockLevel), completion.stockLevel, ns2us(div(time.stockLevel, completion.stockLevel)));
	System.out.println("===============================================================================================");
	System.out.println("     tx type: invocation:completion(:intentional rollback) - retry on statement:retry on commit");
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.printf("   new order: %8d:%8d:%8d - %8d:%8d%n", invocation.newOrder, completion.newOrder, newOrderIntentionalRollback, retryOnStatement.newOrder, retryOnCommit.newOrder);
	System.out.printf("     payment: %8d:%8d          - %8d:%8d%n", invocation.payment, completion.payment, retryOnStatement.payment, retryOnCommit.payment);
	System.out.printf("order status: %8d:%8d          - %8d:%8d%n", invocation.orderStatus, completion.orderStatus, retryOnStatement.orderStatus, retryOnCommit.orderStatus);
	System.out.printf("    delivery: %8d:%8d          - %8d:%8d%n", invocation.delivery, completion.delivery, retryOnStatement.delivery, retryOnCommit.delivery);
	System.out.printf(" stock level: %8d:%8d          - %8d:%8d%n", invocation.stockLevel, completion.stockLevel, retryOnStatement.stockLevel, retryOnCommit.stockLevel);
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.printf("   new order: ORDERS %6d DISTRICT %6d WAREHOUSE %6d CUSTOMER %6d STOCK %6d%n",
			  ordersTable.newOrder, districtTable.newOrder, warehouseTable.newOrder, customerTable.newOrder, stockTable.newOrder);
	System.out.printf("     payment: ORDERS %6d DISTRICT %6d WAREHOUSE %6d CUSTOMER %6d STOCK %6d%n",
			  ordersTable.payment, districtTable.payment, warehouseTable.payment, customerTable.payment, stockTable.payment);
	System.out.printf("order status: ORDERS %6d DISTRICT %6d WAREHOUSE %6d CUSTOMER %6d STOCK %6d%n",
			  ordersTable.orderStatus, districtTable.orderStatus, warehouseTable.orderStatus, customerTable.orderStatus, stockTable.orderStatus);
	System.out.printf("    delivery: ORDERS %6d DISTRICT %6d WAREHOUSE %6d CUSTOMER %6d STOCK %6d%n",
			  ordersTable.delivery, districtTable.delivery, warehouseTable.delivery, customerTable.delivery, stockTable.delivery);
	System.out.printf(" stock level: ORDERS %6d DISTRICT %6d WAREHOUSE %6d CUSTOMER %6d STOCK %6d%n",
			  ordersTable.stockLevel, districtTable.stockLevel, warehouseTable.stockLevel, customerTable.stockLevel, stockTable.stockLevel);
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.printf("##NoTPM=%.2f%n", ((double) completion.newOrder * 60.0 * 1000.0) / ((double) elapsed / (double) n));
    }
}
