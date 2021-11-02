package com.nautilus_technologies.tsubakuro.low;

import java.util.Random;

public class RandomGenerator {
    Random random;

    static {
	var random = new Random();
    }

    public RandomGenerator() {
	this.random = new Random();
    }

    public long uniformWithin(long a, long b) {
	if (a < b) {
	    return a + random.nextInt((int) (b - a + 1));
	}
	return b + random.nextInt((int) (a - b + 1));
    }
}
