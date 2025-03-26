/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
class ResponseBox {
    static final Logger LOG = LoggerFactory.getLogger(ResponseBox.class);

    private final Link link;
    private final int size;

    private final Queues queues;
    private final Queues urgentQueues;
    private SlotEntry[] boxes;
    private boolean intentionalClose = false;

    ResponseBox(@Nonnull Link link, int size, int urgentSize) {
        this.link = link;
        this.size = size;
        boxes = new SlotEntry[size + urgentSize];
        this.queues = new Queues(link);
        this.urgentQueues = new Queues(link);
        for (int i = 0; i < size + urgentSize; i++) {
            boxes[i] = new SlotEntry(i);
            if (i < size) {
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
            link.sendInternal(slotEntry.slot(), header, payload, channelResponse);
            channelResponse.finishAssignSlot(slotEntry.slot());
            return channelResponse;
        }
        var channelResponse = new ChannelResponse(link);
        q.addRequest(new RequestEntry(channelResponse, header, payload));
        return channelResponse;
    }

    void push(int slot, byte[] payload) {
        var slotEntry = boxes[slot];
        var channelResponse = slotEntry.channelResponse();
        if (channelResponse != null) {
            channelResponse.setMainResponse(ByteBuffer.wrap(payload));
            if (slot < size) {
                queues.returnSlot(slotEntry);
            } else {
                urgentQueues.returnSlot(slotEntry);
            }
            return;
        }
        LOG.error("invalid slotEntry is used: slot={}, payload={}", slot, payload);
        throw new AssertionError("invalid slotEntry is used");
    }

    void push(int slot, IOException e) {
        var slotEntry = boxes[slot];
        var channelResponse = slotEntry.channelResponse();
        if (channelResponse != null) {
            channelResponse.setMainResponse(e);
            if (slot < size) {
                queues.returnSlot(slotEntry);
            } else {
                urgentQueues.returnSlot(slotEntry);
            }
            return;
        }
        LOG.error("invalid slotEntry is used: slot={}, exception={}", slot, e);
        throw new AssertionError("invalid slotEntry is used");
    }

    void pushHead(int slot, byte[] payload, ResultSetWire resultSetWire) {
        boxes[slot].channelResponse().setResultSet(ByteBuffer.wrap(payload), resultSetWire);
    }

    void doClose(boolean ic) {
        intentionalClose = ic;
        close();
    }

    void close() {
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
