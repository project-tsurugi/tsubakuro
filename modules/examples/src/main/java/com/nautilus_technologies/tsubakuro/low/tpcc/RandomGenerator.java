package com.nautilus_technologies.tsubakuro.low.tpcc;

import java.util.Random;

public class RandomGenerator {
    Random random;

    static long c255;
    static long c1023;
    static long c8191;

    static {
	var random = new Random();
	long r = random.nextLong();
	c255 = r % (Scale.L_NAMES - 1);
	r = random.nextLong();
	c1023 = (r % Scale.CUSTOMERS) + 1;
	r = random.nextLong();
	c8191 = (r % Scale.ITEMS) + 1;
    }

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
    public long nonUniform255Within(long x, long y) {
	return (((uniformWithin(0, 255) | uniformWithin(x, y)) + c255) % (y - x + 1)) + x;
    }
    public long nonUniform1023Within(long x, long y) {
	return (((uniformWithin(0, 1023) | uniformWithin(x, y)) + c1023) % (y - x + 1)) + x;
    }
    public long nonUniform8191Within(long x, long y) {
	return (((uniformWithin(0, 8191) | uniformWithin(x, y)) + c8191) % (y - x + 1)) + x;
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
