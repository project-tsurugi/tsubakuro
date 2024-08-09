package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * ResponseBox type.
 */
public class ResponseBox {
    private static final int SIZE = Byte.MAX_VALUE;
    private static final int URGENT_SIZE = 2;

    static final Logger LOG = LoggerFactory.getLogger(ResponseBox.class);

    private final Link link;
    private final Queues queues;
    private final Queues urgentQueues;
    private SlotEntry[] boxes = new SlotEntry[SIZE + URGENT_SIZE];
    private boolean intentionalClose = false;

    public ResponseBox(@Nonnull Link link) {
        this.link = link;
        this.queues = new Queues(link);
        this.urgentQueues = new Queues(link);
        for (int i = 0; i < SIZE + URGENT_SIZE; i++) {
            boxes[i] = new SlotEntry(i);
            if (i < SIZE) {
                queues.addSlot(boxes[i]);
            } else {
                urgentQueues.addSlot(boxes[i]);
            }
        }
    }

    ChannelResponse register(@Nonnull byte[] header, @Nonnull byte[] payload) {
        return registerInternal(header, payload, queues);
    }

    ChannelResponse registerUrgent(@Nonnull byte[] header, @Nonnull byte[] payload) throws IOException {
        return registerInternal(header, payload, urgentQueues);
    }

    private ChannelResponse registerInternal(@Nonnull byte[] header, @Nonnull byte[] payload, Queues q) {
        var slotEntry = q.pollSlot();
        if (slotEntry != null) {
            var channelResponse = new ChannelResponse(link, slotEntry.slot());
            slotEntry.channelResponse(channelResponse);
            slotEntry.requestMessage(payload);
            link.send(slotEntry.slot(), header, payload, channelResponse);
            return channelResponse;
        }
        var channelResponse = new ChannelResponse(link);
        q.addRequest(new RequestEntry(channelResponse, header, payload));
        if (!queues.isSlotEmpty()) {
            q.pairAnnihilation();
        }
        return channelResponse;
    }

    public void push(int slot, byte[] payload) {
        var slotEntry = boxes[slot];
        var channelResponse = slotEntry.channelResponse();
        if (channelResponse != null) {
            channelResponse.setMainResponse(ByteBuffer.wrap(payload));
            if (slot < SIZE) {
                returnEntryToQueue(slotEntry, queues);
            } else {
                returnEntryToQueue(slotEntry, urgentQueues);
            }
            return;
        }
        LOG.error("invalid slotEntry is used: slot={}, payload={}", slot, payload);
        throw new AssertionError("invalid slotEntry is used");
    }

    public void push(int slot, IOException e) {
        var slotEntry = boxes[slot];
        var channelResponse = slotEntry.channelResponse();
        if (channelResponse != null) {
            channelResponse.setMainResponse(e);
            if (slot < SIZE) {
                returnEntryToQueue(slotEntry, queues);
            } else {
                returnEntryToQueue(slotEntry, urgentQueues);
            }
            return;
        }
        LOG.error("invalid slotEntry is used: slot={}, exception={}", slot, e);
        throw new AssertionError("invalid slotEntry is used");
    }

    private void returnEntryToQueue(SlotEntry slotEntry, Queues q) {
        var queuedRequest = q.pollRequest();
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
        q.addSlot(slotEntry);
        if (q.isRequestEmpty()) {
            return;
        }
        q.pairAnnihilation();
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
                    response.setMainResponse(new IOException(link.linkLostMessage()));
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
