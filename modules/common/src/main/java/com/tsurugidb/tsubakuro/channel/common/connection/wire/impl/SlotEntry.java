package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import javax.annotation.Nonnull;

public class SlotEntry {
    private final int slot;
    private ChannelResponse channelResponse;
    private byte[] requestMessage;  // for diagnostic

    SlotEntry(int slot) {
        this.slot = slot;
    }

    int slot() {
        return slot;
    }

    void channelResponse(@Nonnull ChannelResponse cr) {
        channelResponse = cr;
    }

    void resetChannelResponse() {
        channelResponse = null;
    }

    ChannelResponse channelResponse() {
        return channelResponse;
    }

    // for diagnostic
    void requestMessage(byte[] rq) {
        requestMessage = rq;
    }
    byte[] requestMessage() {
        return requestMessage;
    }
}
