package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.util.Objects;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = 16;

    private StreamWire streamWire;
    private byte[][] firstResponse;
    private byte[][] secondResponse;
    private int[] expected;
    private int[] used;

    public ResponseBox(StreamWire streamWire) {
	this.streamWire = streamWire;
	this.firstResponse = new byte[SIZE][];
	this.secondResponse = new byte[SIZE][];
	this.expected = new int[SIZE];
	this.used = new int[SIZE];
	for (int i = 0; i < SIZE; i++) {
	    firstResponse[i] = null;
	    secondResponse[i] = null;
	    expected[i] = 0;
	    used[i] = 0;
	}
    }

    public byte[] receive(int index) throws IOException {
	while (true) {
	    streamWire.receive();
	    var slot = streamWire.getInfo();
	    if (Objects.isNull(firstResponse[slot])) {
		firstResponse[slot] = streamWire.getBytes();
	    } else {
		secondResponse[slot] = streamWire.getBytes();
	    }
	    streamWire.release();

	    if (used[index] == 0) {
		if (Objects.nonNull(firstResponse[index])) {
		    return firstResponse[index];
		}
	    } else {
		if (Objects.nonNull(secondResponse[index])) {
		    return secondResponse[index];
		}
	    }
	}
    }

    public byte lookFor(int n) {
	synchronized (this) {
	    for (byte i = 0; i < SIZE; i++) {
		if (expected[i] == 0) {
		    expected[i] = n;
		    return i;
		}
	    }
	    return -1;
	}
    }

    public void unreceive(int index) {
	if (expected[index] == 0) {
	    System.err.println("unused slot");
	}
	if (expected[index] != 2) {
	    System.err.println("not a slot for query");
	}
	if (used[index] == 0) {
	    System.err.println("unused slot");
	}
	expected[index]--;
	used[index]--;
    }

    public void release(int index) {
	if (used[index] == 0) {
	    used[index]++;
	    if (expected[index] == used[index]) {
		expected[index] = 0;
		used[index] = 0;
		firstResponse[index] = null;
	    }
	} else {
	    used[index]++;
	    if (expected[index] == used[index]) {
		expected[index] = 0;
		used[index] = 0;
		firstResponse[index] = null;
		secondResponse[index] = null;
	    }
	}
    }
}
