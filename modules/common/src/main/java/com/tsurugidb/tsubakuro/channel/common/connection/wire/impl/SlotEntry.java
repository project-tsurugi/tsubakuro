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
