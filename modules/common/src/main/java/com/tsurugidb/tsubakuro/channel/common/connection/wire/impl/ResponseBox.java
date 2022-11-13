package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = Byte.MAX_VALUE;

    private final Link link;
    private final Queues queues;
    private SlotEntry[] boxes = new SlotEntry[SIZE];

    public ResponseBox(@Nonnull Link link) {
        this.link = link;
        this.queues = new Queues(link);
        for (byte i = 0; i < SIZE; i++) {
            boxes[i] = new SlotEntry(i);
            queues.addSlot(boxes[i]);
        }
    }

    public ChannelResponse register(@Nonnull byte[] header, @Nonnull byte[] payload) {
        var channelResponse = new ChannelResponse();
        var slotEntry = queues.pollSlot();
        if (Objects.nonNull(slotEntry)) {
            slotEntry.channelResponse(channelResponse);
            link.send(slotEntry.slot(), header, payload, channelResponse);
        } else {
            queues.addRequest(new RequestEntry(channelResponse, header, payload));
            if (!queues.isSlotEmpty()) {
                queues.pairAnnihilation();
            }
        }
        return channelResponse;
    }

    public void push(int slot, byte[] payload) {
        var slotEntry = boxes[slot];
        slotEntry.channelResponse().setMainResponse(ByteBuffer.wrap(payload));
        var queuedRequest = queues.pollRequest();
        if (Objects.nonNull(queuedRequest)) {
            link.send(slot, queuedRequest.header(), queuedRequest.payload(), queuedRequest.channelResponse());
        } else {
            queues.addSlot(slotEntry);
            if (queues.isRequestEmpty()) {
                return;
            }
            queues.pairAnnihilation();
        }
    }

    public void pushHead(int slot, byte[] payload, ResultSetWire resultSetWire) throws IOException {
        boxes[slot].channelResponse().setResultSet(ByteBuffer.wrap(payload), resultSetWire);
    }

    public static int responseBoxSize() {
        return SIZE;
    }
}
