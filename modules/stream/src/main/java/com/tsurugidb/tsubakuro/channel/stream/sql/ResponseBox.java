package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.util.Objects;
<<<<<<< HEAD
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.stream.StreamWire;
=======
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
>>>>>>> d28a5e1... implement and use StreamWireReceiver

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = Byte.MAX_VALUE;

    private Abox[] boxes;

    private static class Abox {
<<<<<<< HEAD
        private AtomicBoolean available;
//        private ChannelResponse channelResponse;
=======
        private Lock lock = new ReentrantLock();
        private Condition availableCondition = lock.newCondition();
>>>>>>> d28a5e1... implement and use StreamWireReceiver
        private byte[] firstResponse;
        private byte[] secondResponse;
        private int expected;
        private int used;

        Abox() {
            clear();
        }

        void clear() {
            firstResponse = null;
            secondResponse = null;
            expected = 0;
            used = 0;
        }
    }

    public ResponseBox() {
        this.boxes = new Abox[SIZE];

        for (int i = 0; i < SIZE; i++) {
            boxes[i] = new Abox();
        }
    }

    public byte[] receive(int slot) throws IOException {
        while (true) {
            Lock l =  boxes[slot].lock;
            l.lock();
            try {
                if (boxes[slot].used == 0) {
                    if (Objects.nonNull(boxes[slot].firstResponse)) {
                        return boxes[slot].firstResponse;
                    }
                } else {
                    if (Objects.nonNull(boxes[slot].secondResponse)) {
                        return boxes[slot].secondResponse;
                    }
                }
                boxes[slot].availableCondition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                l.unlock();
            }
        }
    }

    public void push(int slot, byte[] payload, boolean head) {
        Lock l =  boxes[slot].lock;
        l.lock();
        try {
            if (Objects.isNull(boxes[slot].firstResponse)) {
                boxes[slot].firstResponse = payload;
            } else {
                boxes[slot].secondResponse = payload;
            }
            if (head) {
                boxes[slot].expected = 2;
            }
            boxes[slot].availableCondition.signal();
        } finally {
            l.unlock();
        }
    }

    public byte lookFor(ChannelResponse channelResponse) {
        synchronized (this) {
            for (byte i = 0; i < SIZE; i++) {
                if (boxes[i].expected == 0) {
                    boxes[i].expected = 1;
//                    boxes[i].channelResponse = channelResponse;
                    return i;
                }
            }
            return -1;
        }
    }

<<<<<<< HEAD
=======
    public void setResultSetMode(int slot) {
        Lock l =  boxes[slot].lock;
        l.lock();
        try {
            boxes[slot].expected = 2;
        } finally {
            l.unlock();
        }
    }

>>>>>>> d28a5e1... implement and use StreamWireReceiver
    public void release(int slot) {
        Lock l =  boxes[slot].lock;
        l.lock();
        try {
            boxes[slot].used++;
            if (boxes[slot].expected == boxes[slot].used) {
                boxes[slot].clear();
            }
        } finally {
            l.unlock();
        }
    }

    public static byte responseBoxSize() {
        return (byte) SIZE;
    }
}
