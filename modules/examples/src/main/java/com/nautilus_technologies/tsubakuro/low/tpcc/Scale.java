package com.nautilus_technologies.tsubakuro.low.tpcc;

public final class Scale {
    private Scale() {
    }

    public static long warehouses() {
	return 1;
    }
    public static long items() {
	return 100000;
    }
    public static long districts() {
	return 10;
    }
    public static long customers() {
	return 3000;
    }
    public static long orders() {
	return 3000;
    }
    public static long minOlCount() {
	return 5;
    }
    public static long maxOlCount() {
	return 15;
    }
    public static long maxOl() {
	return maxOlCount() + 1;
    }
    public static long lnames() {
	return 1000;
    }
}
