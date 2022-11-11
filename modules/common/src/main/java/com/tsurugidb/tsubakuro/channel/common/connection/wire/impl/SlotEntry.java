package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import javax.annotation.Nonnull;

public class SlotEntry {
    private final int slot;
    private ChannelResponse channelResponse;

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
}
