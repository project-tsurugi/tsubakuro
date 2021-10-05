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
    public Counter districtTable;
    public Counter warehouseTable;
    public Counter ordersTable;
    public Counter customerTable;
    public Counter stockTable;
    public long newOrderIntentionalRollback;
    public long elapsed;
    public long count;

    public Profile() {
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
    }
    public void add(Profile profile) {
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
    }
    public void print(int threads) {
	System.out.println("duration(mS): " + elapsed);
	System.out.println("     tx type: invocation:completion(:intentional rollback) - retry on statement:retry on commit");
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.printf("   new order: %d:%d:%d - %d:%d\n", invocation.newOrder, completion.newOrder, newOrderIntentionalRollback, retryOnStatement.newOrder, retryOnCommit.newOrder);
	System.out.printf("     payment: %d:%d - %d:%d\n", invocation.payment, completion.payment, retryOnStatement.payment, retryOnCommit.payment);
	System.out.printf("    delivery: %d:%d - %d:%d\n", invocation.delivery, completion.delivery, retryOnStatement.delivery, retryOnCommit.delivery);
	System.out.printf("order status: %d:%d - %d:%d\n", invocation.orderStatus, completion.orderStatus, retryOnStatement.orderStatus, retryOnCommit.orderStatus);
	System.out.printf(" stock level: %d:%d - %d:%d\n", invocation.stockLevel, completion.stockLevel, retryOnStatement.stockLevel, retryOnCommit.stockLevel);
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.printf("   new order: ORDERS %d DISTRICT %d WAREHOUSE %d CUSTOMER %d STOCK %d\n",
			  ordersTable.newOrder, districtTable.newOrder, warehouseTable.newOrder, customerTable.newOrder, stockTable.newOrder);
	System.out.printf("     payment: ORDERS %d DISTRICT %d WAREHOUSE %d CUSTOMER %d STOCK %d\n",
			  ordersTable.payment, districtTable.payment, warehouseTable.payment, customerTable.payment, stockTable.payment);
	System.out.printf("    delivery: ORDERS %d DISTRICT %d WAREHOUSE %d CUSTOMER %d STOCK %d\n",
			  ordersTable.delivery, districtTable.delivery, warehouseTable.delivery, customerTable.delivery, stockTable.delivery);
	System.out.printf("order status: ORDERS %d DISTRICT %d WAREHOUSE %d CUSTOMER %d STOCK %d\n",
			  ordersTable.orderStatus, districtTable.orderStatus, warehouseTable.orderStatus, customerTable.orderStatus, stockTable.orderStatus);
	System.out.printf(" stock level: ORDERS %d DISTRICT %d WAREHOUSE %d CUSTOMER %d STOCK %d\n",
			  ordersTable.stockLevel, districtTable.stockLevel, warehouseTable.stockLevel, customerTable.stockLevel, stockTable.stockLevel);
	System.out.println("-----------------------------------------------------------------------------------------------");
	System.out.printf("##NoTPM=%.2f\n", ((double) completion.newOrder * 60.0 * 1000.0) / ((double) elapsed / (double) threads));
    }
}
