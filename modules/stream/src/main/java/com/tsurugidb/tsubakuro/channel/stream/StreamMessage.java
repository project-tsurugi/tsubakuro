package com.tsurugidb.tsubakuro.channel.stream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class StreamMessage {
    public final byte[] bytes;
    private final byte info;
    private final int slot;
    private final byte writer;

    public StreamMessage(byte info, byte[] bytes, int slot, byte writer) throws IOException {
        this.info = info;
        this.bytes = bytes;
        this.slot = slot;
        this.writer = writer;
    }

    public byte getInfo() {  // used only by FutureSessionWireImpl
        return info;
    }
    public String getString() {  // used only by FutureSessionWireImpl
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
