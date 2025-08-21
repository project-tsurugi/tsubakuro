/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.mock;

import java.io.IOException;

import com.tsurugidb.sql.proto.SqlResponse;

/**
 * ResponseMessage type.
 */
public final class ResponseMessage {

    private int slot;
    private final byte info;
    private final byte[] body;
    private final IOException e;
    private final byte[] bodyHead;
    private boolean isBodyHead;

    ResponseMessage(byte[] responseMessage) {
        this.slot = -1;
        this.body = responseMessage;
        this.bodyHead = null;
        this.isBodyHead = false;
        this.info = MockLink.RESPONSE_PAYLOAD;
        this.e = null;
    }

    ResponseMessage(byte[] responseMessage, byte[] bodyHead) {
        this.slot = -1;
        this.body = responseMessage;
        this.bodyHead = bodyHead;
        this.isBodyHead = true;
        this.info = MockLink.RESPONSE_PAYLOAD;
        this.e = null;
    }

    ResponseMessage(IOException e) {
        this.slot = -1;
        this.body = null;
        this.bodyHead = null;
        this.isBodyHead = false;
        this.info = MockLink.RESPONSE_NULL;
        this.e = e;
    }

    void assignSlot(int slot) {
        this.slot = slot;
    }

    int getSlot() {
        return slot;
    }

    byte getInfo() {
        return info;
    }

    byte[] getBytes() {
        return body;
    }

    IOException getIOException() {
        return e;
    }

    boolean hasBodyHead() {
        return isBodyHead;
    }

    byte[] getBodyHead() {
        isBodyHead = false;
        return bodyHead;
    }
}
