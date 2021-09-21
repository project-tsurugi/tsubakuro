package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.util.Random;

public class RandomGenerator {
    Random random;
    
    public RandomGenerator() {
	this.random = new Random();
    }

    public long uniformWithin(long a, long b) {
	long r = random.nextLong();
	if (a < b) {
	    long s = r % (b - a + 1);
	    if (s < 0) {
		s += (b - a + 1);
	    }
	    return a + s;
	}
	long s = r % (a - b + 1);
	if (s < 0) {
	    s += (a - b + 1);
	}
	return b + s;
    }    
    public long nonUniformWithin(long a, long x, long y) {
	long c = uniformWithin(0, a);
	return (((uniformWithin(0, a) | uniformWithin(x, y)) + c) % (y - x + 1)) + x;
    }
    public String makeAlphaString(long min, long max) {
	char character = 'a';
	String ss = "";

	var length = uniformWithin(min, max);
	for (long i = 0; i < length;  ++i) {
	    ss += character + (int) uniformWithin(0, 25);
	}
	return ss;
    }
}
