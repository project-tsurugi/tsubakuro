package com.tsurugidb.tsubakuro.channel.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.ChannelResponse;

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = Byte.MAX_VALUE;

    private final StreamWire streamWire;
    private Abox[] boxes;

    private static class Abox {
        private ChannelResponse channelResponse;
        private final Lock lock = new ReentrantLock();
        private final Condition availableCondition = lock.newCondition();
        private int expected;
        private int received;

        Abox(@Nonnull ChannelResponse channelResponse) {
            this.channelResponse = channelResponse;
            this.expected = 1;
            this.received = 0;
        }

        ChannelResponse channelResponse() {
            return channelResponse;
        }
    }

    public ResponseBox(@Nonnull StreamWire streamWire) {
        this.boxes = new Abox[SIZE];
        this.streamWire = streamWire;

        for (int i = 0; i < SIZE; i++) {
            boxes[i] = null;
        }
    }

    public ChannelResponse register(@Nonnull byte[] header, @Nonnull byte[] payload) throws IOException {
        int slot = lookFor();
        var channelResponse = new ChannelResponse();
        boxes[slot] = new Abox(channelResponse);
        synchronized (this) {
            streamWire.send(slot, header, payload);
        }
        return channelResponse;
    }

    public void push(int slot, byte[] payload, boolean head) {
        Lock l =  boxes[slot].lock;
        l.lock();
        try {
            var box = boxes[slot];
            if (head) {
                box.expected = 2;
            }
            if (box.received == 0) {
                box.channelResponse().setMainResponse(ByteBuffer.wrap(payload));
            } else {
                box.channelResponse().setSecondResponse(ByteBuffer.wrap(payload));
            }
            box.received++;
            if (box.expected == box.received) {
                boxes[slot] = null;
            }
        } finally {
            l.unlock();
        }
    }

    public int lookFor() {
        synchronized (this) {
            for (byte i = 0; i < SIZE; i++) {
                if (Objects.isNull(boxes[i])) {
                    return i;
                }
            }
            return -1;
        }
    }

    public static byte responseBoxSize() {
        return (byte) SIZE;
    }
}
