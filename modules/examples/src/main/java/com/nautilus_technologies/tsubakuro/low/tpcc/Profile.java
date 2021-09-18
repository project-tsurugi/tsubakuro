package com.nautilus_technologies.tsubakuro.low.tpcc;

public class Profile {
    public long warehouses;
    public long index;
    long newOrder;
    long payment;
    long delivery;
    long orderStatus;
    long stockLevel;

    public Profile() {
	this.newOrder = 0;
	this.payment = 0;
	this.delivery = 0;
	this.orderStatus = 0;
	this.stockLevel = 0;
    }
}
