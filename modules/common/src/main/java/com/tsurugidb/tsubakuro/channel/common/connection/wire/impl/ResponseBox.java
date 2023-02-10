package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

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
            slotEntry.requestMessage(payload);
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
            slotEntry.channelResponse(queuedRequest.channelResponse());
            slotEntry.requestMessage(queuedRequest.payload());
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

    public void close() {
        for (SlotEntry e : boxes) {
            var response = e.channelResponse();
            if (Objects.nonNull(response)) {
                response.setMainResponse(new IOException("Server crashed"));
            }
        }
    }

    public static int responseBoxSize() {
        return SIZE;
    }

    // for diagnostic
    String diagnosticInfo() {
        String diagnosticInfo = "";
        for (var et : boxes) {
            var cr = et.channelResponse();
            if (Objects.nonNull(cr)) {
                try {
                    diagnosticInfo += "  +request in processing: " + SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(ByteBuffer.wrap(et.requestMessage()))).toString() + cr.diagnosticInfo() + System.getProperty("line.separator");
                } catch (IOException ex) {
                    diagnosticInfo += "  +request in processing: (error) " + ex + System.getProperty("line.separator");
                }
            }
        }
        return diagnosticInfo;
    }
}
