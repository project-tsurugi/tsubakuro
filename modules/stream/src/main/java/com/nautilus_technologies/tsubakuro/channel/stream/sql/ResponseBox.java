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

    public byte[] receive(int slot) throws IOException {
        while (true) {
            if (used[slot] == 0) {
                if (Objects.nonNull(firstResponse[slot])) {
                    return firstResponse[slot];
                }
            } else {
                if (Objects.nonNull(secondResponse[slot])) {
                    return secondResponse[slot];
                }
            }
            streamWire.pull();
        }
    }

    public void push(int slot, byte[] payload) {
        if (Objects.isNull(firstResponse[slot])) {
            firstResponse[slot] = payload;
        } else {
            secondResponse[slot] = payload;
        }
    }

    public byte lookFor() {
        synchronized (this) {
            for (byte i = 0; i < SIZE; i++) {
                if (expected[i] == 0) {
                    expected[i] = 1;
                    return i;
                }
            }
            return -1;
        }
    }

    public void setQueryMode(int slot) {
        expected[slot] = 2;
    }

    public void unreceive(int slot) {
        if (expected[slot] == 0) {
            System.err.println("unused slot");
        }
        if (expected[slot] != 2) {
            System.err.println("not a slot for query");
        }
        if (used[slot] == 0) {
            System.err.println("unused slot");
        }
        expected[slot]--;
        used[slot]--;
    }

    public void release(int slot) {
        if (used[slot] == 0) {
            used[slot]++;
            if (expected[slot] == used[slot]) {
                expected[slot] = 0;
                used[slot] = 0;
                firstResponse[slot] = null;
            }
        } else {
            used[slot]++;
            if (expected[slot] == used[slot]) {
                expected[slot] = 0;
                used[slot] = 0;
                firstResponse[slot] = null;
                secondResponse[slot] = null;
            }
        }
    }
}
