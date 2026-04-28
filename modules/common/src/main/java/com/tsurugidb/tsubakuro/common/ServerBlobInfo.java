/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.common;

import java.util.Optional;

/**
 * An abstract super interface of BLOB data to send to Tsurugi server.
 *
 * @since 1.11.0
 */
public class ServerBlobInfo {
    private final String channelName;
    private final String serverPath;

    /**
     * Creates a new instance of {@code ServerBlobInfo}.
     *
     * @param channelName the channel name for sending this BLOB data
     * @param serverPath the path of the server file that represents this BLOB data
     */
    public ServerBlobInfo(String channelName, String serverPath) {
        this.channelName = channelName;
        this.serverPath = serverPath;
    }

    /**
     * Returns the channel name for sending this BLOB data.
     * <p>
     * The channel name is used to identify the BLOB data in the server side,
     * so that it must be unique in the same request.
     * </p>
     * @return the channel name
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Returns the path of the server file that represents this BLOB data.
     * @return the path of the server file, or empty if it does not exist
     */
    public Optional<String> getPath() {
        return Optional.ofNullable(serverPath);
    }
}
