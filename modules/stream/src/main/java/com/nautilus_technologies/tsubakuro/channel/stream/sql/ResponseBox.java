package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = 16;

    private StreamWire streamWire;
    private Abox[] boxes;

    private static class Abox {
        private AtomicBoolean available;
        private byte[] firstResponse;
        private byte[] secondResponse;
        private int expected;
        private int used;

        Abox() {
            available = new AtomicBoolean();
            clear();
        }

        void clear() {
            available.set(false);
            firstResponse = null;
            secondResponse = null;
            expected = 0;
            used = 0;
        }
    }

    public ResponseBox(StreamWire streamWire) {
        this.streamWire = streamWire;
        this.boxes = new Abox[SIZE];

        for (int i = 0; i < SIZE; i++) {
            boxes[i] = new Abox();
        }
    }

    public byte[] receive(int slot) throws IOException {
        while (true) {
            synchronized (boxes[slot]) {
                if (boxes[slot].used == 0) {
                    if (Objects.nonNull(boxes[slot].firstResponse)) {
                        return boxes[slot].firstResponse;
                    }
                } else {
                    if (Objects.nonNull(boxes[slot].secondResponse)) {
                        return boxes[slot].secondResponse;
                    }
                }
            }
            streamWire.pull(boxes[slot].available);
        }
    }

    public void push(int slot, byte[] payload) {
        synchronized (boxes[slot]) {
            if (Objects.isNull(boxes[slot].firstResponse)) {
                boxes[slot].firstResponse = payload;
            } else {
                boxes[slot].secondResponse = payload;
            }
            boxes[slot].available.set(true);
        }
    }

    public byte lookFor() {
        synchronized (this) {
            for (byte i = 0; i < SIZE; i++) {
                if (boxes[i].expected == 0) {
                    boxes[i].expected = 1;
                    return i;
                }
            }
            return -1;
        }
    }

    public void setResultSetMode(int slot) {
        synchronized (boxes[slot]) {
            boxes[slot].expected = 2;
        }
    }

    public void release(int slot) {
        synchronized (boxes[slot]) {
            boxes[slot].used++;
            if (boxes[slot].expected == boxes[slot].used) {
                boxes[slot].clear();
            }
        }
    }
}
