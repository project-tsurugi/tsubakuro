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

import java.io.UnsupportedEncodingException;

/**
 * LinkMessage type.
 */
public class LinkMessage {
    /**
     * The message body.
     */
    public final byte[] bytes;

    private final byte info;
    private final int slot;
    private final byte writer;

    /**
     * Constructor.
     * @param info the message type
     * @param bytes the message body
     * @param slot the slot number
     * @param writer the writer id
     */
    public LinkMessage(byte info, byte[] bytes, int slot, byte writer) {
        this.info = info;
        this.bytes = bytes;
        this.slot = slot;
        this.writer = writer;
    }

    /**
     * Constructor.
     * @param info the message type
     * @param bytes the message body
     * @param slot the slot number
     */
    public LinkMessage(byte info, byte[] bytes, int slot) {
        this.info = info;
        this.bytes = bytes;
        this.slot = slot;
        this.writer = 0;
    }

    /**
     * Getters.
     * @return the info
     */
    public byte getInfo() {  // used only by FutureWireImpl
        return info;
    }

    /**
     * Getters.
     * @return the string
     */
    public String getString() {  // used only by FutureWireImpl
        try {
            if (bytes != null) {
                return new String(bytes, "UTF-8");
            }
        } catch (UnsupportedEncodingException e) {
            // As long as only alphabetic and numeric characters are received,
            // this exception will never occur.
            System.err.println(e);
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Returns the bytes.
     * @return the bytes
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Returns the slot.
     * @return the slot
     */
    public int getSlot() {
        return slot;
    }

    /**
     * Returns the writer.
     * @return the writer
     */
    public byte getWriter() {
        return writer;
    }
}
