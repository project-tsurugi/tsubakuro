package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.UnsupportedEncodingException;

public class LinkMessage {
    public final byte[] bytes;
    private final byte info;
    private final int slot;
    private final byte writer;

    public LinkMessage(byte info, byte[] bytes, int slot, byte writer) {
        this.info = info;
        this.bytes = bytes;
        this.slot = slot;
        this.writer = writer;
    }

    public LinkMessage(byte info, byte[] bytes, int slot) {
        this.info = info;
        this.bytes = bytes;
        this.slot = slot;
        this.writer = 0;
    }

    public byte getInfo() {  // used only by FutureWireImpl
        return info;
    }
    public String getString() {  // used only by FutureWireImpl
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // As long as only alphabetic and numeric characters are received,
            // this exception will never occur.
            System.err.println(e);
            e.printStackTrace();
        }
        return "";
    }
    public byte[] getBytes() {
        return bytes;
    }
    public int getSlot() {
        return slot;
    }
    public byte getWriter() {
        return writer;
    }
}
