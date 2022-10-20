package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = Byte.MAX_VALUE;
    private static final int INVALID_SLOT = -1;

    private static class Abox {
        private final ChannelResponse channelResponse;
        private final Lock lock = new ReentrantLock();
        private final Condition availableCondition = lock.newCondition();

        Abox(@Nonnull ChannelResponse channelResponse) {
            this.channelResponse = channelResponse;
        }

        ChannelResponse channelResponse() {
            return channelResponse;
        }
    }

    private final Link link;
    private AtomicReferenceArray<Abox> boxes = new AtomicReferenceArray<>(SIZE);

    public ResponseBox(@Nonnull Link link) {
        this.link = link;
    }

    public ChannelResponse register(@Nonnull byte[] header, @Nonnull byte[] payload) throws IOException {
        var channelResponse = new ChannelResponse();
        var box = new Abox(channelResponse);
        int slot = INVALID_SLOT;
        for (byte i = 0; i < SIZE; i++) {
            if (Objects.isNull(boxes.get(i))) {
                if (boxes.compareAndSet(i, null, box)) {
                    slot = i;
                    break;
                }
            }
        }
        if (slot == INVALID_SLOT) {
            throw new IOException("no available response box");
        }
        link.send(slot, header, payload);
        return channelResponse;
    }

    public void push(int slot, byte[] payload) {
        Lock l =  boxes.get(slot).lock;
        l.lock();
        try {
            var box = boxes.get(slot);
            box.channelResponse().setMainResponse(ByteBuffer.wrap(payload));
            boxes.set(slot, null);
        } finally {
            l.unlock();
        }
    }

    public void pushHead(int slot, byte[] payload, ResultSetWire resultSetWire) throws IOException {
        Lock l =  boxes.get(slot).lock;
        l.lock();
        try {
            var box = boxes.get(slot);
            box.channelResponse().setResultSet(ByteBuffer.wrap(payload), resultSetWire);
        } finally {
            l.unlock();
        }
    }

    public static int responseBoxSize() {
        return SIZE;
    }
}
