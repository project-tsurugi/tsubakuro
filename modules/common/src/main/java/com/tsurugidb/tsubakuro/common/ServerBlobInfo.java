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

/**
 * An abstract super interface of BLOB data to send to Tsurugi server.
 *
 * @since 1.11.0
 */
public class ServerBlobInfo {
    private final String channelName;
    private final String path;

    /**
     * Creates a new instance of {@code ServerBlobInfo}.
     *
     * @param channelName the channel name for sending this BLOB data
     * @param path the path of the server file that represents this BLOB data
     */
    public ServerBlobInfo(String channelName, String path) {
        this.channelName = channelName;
        this.path = path;
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
     * Returns the path string of the file that represents this BLOB data.
     * @return the path string of the file
     */
    public String getPath() {
        return path;
    }
}