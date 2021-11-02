package com.nautilus_technologies.tsubakuro.low;

public class Profile {
    public long warehouses;
    public long count;
    public long elapsed;

    public Profile(long warehouses) {
	this.warehouses = warehouses;
	this.count = 0;
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
    public void print() {
	System.out.println("====");
	System.out.printf("%d / %d = %d\n", ns2us(elapsed), count, ns2us(elapsed / count));
    }
}
