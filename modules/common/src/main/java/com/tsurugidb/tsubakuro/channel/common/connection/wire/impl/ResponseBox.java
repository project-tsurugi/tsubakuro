package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = Byte.MAX_VALUE;

    private final Link link;
    private final Queues queues;
    private SlotEntry[] boxes = new SlotEntry[SIZE];
    private boolean intentionalClose = false;

    public ResponseBox(@Nonnull Link link) {
        this.link = link;
        this.queues = new Queues(link);
        for (byte i = 0; i < SIZE; i++) {
            boxes[i] = new SlotEntry(i);
            queues.addSlot(boxes[i]);
        }
    }

    public ChannelResponse register(@Nonnull byte[] header, @Nonnull byte[] payload) {
        var slotEntry = queues.pollSlot();
        if (slotEntry != null) {
            var channelResponse = new ChannelResponse(link, slotEntry.slot());
            slotEntry.channelResponse(channelResponse);
            slotEntry.requestMessage(payload);
            link.send(slotEntry.slot(), header, payload, channelResponse);
            return channelResponse;
        }
        var channelResponse = new ChannelResponse(link);
        queues.addRequest(new RequestEntry(channelResponse, header, payload));
        if (!queues.isSlotEmpty()) {
            queues.pairAnnihilation();
        }
        return channelResponse;
    }

    public void push(int slot, byte[] payload) {
        var slotEntry = boxes[slot];
        slotEntry.channelResponse().setMainResponse(ByteBuffer.wrap(payload));
        returnEntryToQueue(slotEntry);
    }

    public void push(int slot, IOException e) {
        var slotEntry = boxes[slot];
        slotEntry.channelResponse().setMainResponse(e);
        returnEntryToQueue(slotEntry);
    }

    private void returnEntryToQueue(SlotEntry slotEntry) {
        var queuedRequest = queues.pollRequest();
        if (queuedRequest != null) {
            var channelResponse = queuedRequest.channelResponse();
            var slot = slotEntry.slot();
            if (channelResponse.assignSlot(slot)) {
                slotEntry.channelResponse(channelResponse);
                var payload = queuedRequest.payload();
                slotEntry.requestMessage(payload);
                link.send(slot, queuedRequest.header(), payload, channelResponse);
            }
            return;
        }
        queues.addSlot(slotEntry);
        if (queues.isRequestEmpty()) {
            return;
        }
        queues.pairAnnihilation();
    }

    public void pushHead(int slot, byte[] payload, ResultSetWire resultSetWire) throws IOException {
        boxes[slot].channelResponse().setResultSet(ByteBuffer.wrap(payload), resultSetWire);
    }

    public void doClose(boolean ic) {
        intentionalClose = ic;
        close();
    }

    public void close() {
        for (SlotEntry e : boxes) {
            var response = e.channelResponse();
            if (response != null) {
                if (intentionalClose) {
                    response.setMainResponse(new IOException("The wire was closed before receiving a response to this request"));
                } else {
                    response.setMainResponse(new IOException("Server crashed"));
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
        for (var et : boxes) {
            var cr = et.channelResponse();
            if (cr != null) {
                try {
                    diagnosticInfo += "  +request in processing:" + System.getProperty("line.separator") + SqlRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(ByteBuffer.wrap(et.requestMessage()))).toString() + cr.diagnosticInfo() + System.getProperty("line.separator");
                } catch (IOException ex) {
                    diagnosticInfo += "  +request in processing: (error) " + ex + System.getProperty("line.separator");
                }
            }
        }
        return diagnosticInfo;
    }
}
