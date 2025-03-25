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
package com.tsurugidb.tsubakuro.mock;

/**
 * ResponseMessage type.
 */
public final class ResponseMessage {

    private final int slot;
    private final byte info;
    private final byte[] responseMessage;

    public ResponseMessage(byte[] responseMessage) {
        this.slot = -1;
        this.responseMessage = responseMessage;
        this.info = MockLink.RESPONSE_PAYLOAD;
    }

    public ResponseMessage(int slot, ResponseMessage responseMessage) {
        this.slot = slot;
        this.info = responseMessage.getInfo();
        this.responseMessage = responseMessage.getBytes();
    }

    int getSlot() {
        return slot;
    }

    byte getInfo() {
        return info;
    }

    byte[] getBytes() {
        return responseMessage;
    }
}
