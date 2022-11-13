package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

class RequestEntry {
    private final ChannelResponse channelResponse;
    final byte[] header;
    final byte[] payload;

    RequestEntry(ChannelResponse channelResponse, byte[] header, byte[] payload) {
        this.channelResponse = channelResponse;
        this.header = header;
        this.payload = payload;
    }

    ChannelResponse channelResponse() {
        return channelResponse;
    }

    byte[] header() {
        return header;
    }

    byte[] payload() {
        return payload;
    }
}
