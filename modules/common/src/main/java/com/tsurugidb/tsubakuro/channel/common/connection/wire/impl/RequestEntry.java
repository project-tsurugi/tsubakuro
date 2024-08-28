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
