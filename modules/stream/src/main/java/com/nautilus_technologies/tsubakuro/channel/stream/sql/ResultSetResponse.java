package com.nautilus_technologies.tsubakuro.channel.stream.sql;

/**
 * ResultSetResponse type.
 */
public class ResultSetResponse {
    private int info;
    private int writerId;
    public byte[] payload;

    ResultSetResponse(int writerId, byte[] payload) {
    this.writerId = writerId;
    this.payload = payload;
    }
    ResultSetResponse(int info) {
    this.info = info;
    this.payload = null;
    }
    public int getInfo() {
    return info;
    }
    public int getWriterId() {
    return writerId;
    }
    public byte[] getPayload() {
    return payload;
    }
}
