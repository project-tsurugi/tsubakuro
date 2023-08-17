package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

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
        private byte[] requestMessage;  // for diagnostic

        Abox(@Nonnull ChannelResponse channelResponse, byte[] payload) {
            this.channelResponse = channelResponse;
            this.requestMessage = payload; // for diagnostic
        }

        ChannelResponse channelResponse() {
            return channelResponse;
        }

        // for diagnostic
        byte[] requestMessage() {
            return requestMessage;
        }
    }

    private final Link link;
    private AtomicReferenceArray<Abox> boxes = new AtomicReferenceArray<>(SIZE);
    private boolean intentionalClose = false;

    public ResponseBox(@Nonnull Link link) {
        this.link = link;
    }

    public ChannelResponse register(@Nonnull byte[] header, @Nonnull byte[] payload) throws IOException {
        var channelResponse = new ChannelResponse(link);
        var box = new Abox(channelResponse, payload);
        int slot = INVALID_SLOT;
        for (byte i = 0; i < SIZE; i++) {
            if (boxes.get(i) == null) {
                if (boxes.compareAndSet(i, null, box)) {
                    slot = i;
                    break;
                }
            }
        }
        if (slot == INVALID_SLOT) {
            throw new IOException("no available response box");
        }
        link.send(slot, header, payload, channelResponse);
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

    public void push(int slot, IOException e) {
        Lock l =  boxes.get(slot).lock;
        l.lock();
        try {
            var box = boxes.get(slot);
            box.channelResponse().setMainResponse(e);
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

    public void doClose(boolean ic) {
        intentionalClose = ic;
        close();
    }

    public void close() {
        for (byte i = 0; i < SIZE; i++) {
            var box = boxes.get(i);
            if (box != null) {
                var response = box.channelResponse();
                if (response != null) {
                    if (intentionalClose) {
                        response.setMainResponse(new IOException("The wire was closed before receiving a response to this request"));
                    } else {
                        response.setMainResponse(new IOException("Server crashed"));
                    }
                }
            }
        }
    }

    public static int responseBoxSize() {
        return SIZE;
    }

    // for diagnostic
    String diagnosticInfo() {
        String diagnosticInfo = "";
        for (byte i = 0; i < SIZE; i++) {
            var box = boxes.get(i);
            var cr = box.channelResponse();
            if (cr != null) {
                try {
                    diagnosticInfo += "  +request in processing:" + System.getProperty("line.separator") + SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(ByteBuffer.wrap(box.requestMessage()))).toString() + cr.diagnosticInfo() + System.getProperty("line.separator");
                } catch (IOException ex) {
                    diagnosticInfo += "  +request in processing: (error) " + ex + System.getProperty("line.separator");
                }
            }
        }
        return diagnosticInfo;
    }
}
