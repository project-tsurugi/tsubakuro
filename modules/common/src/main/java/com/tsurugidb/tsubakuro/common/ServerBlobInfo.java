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
    private final String serverPath;
    private final BlobRelayReference blobRelayReference;
    private final BlobInfoKind blobInfoKind;

    /**
     * The kind of this ServerBlobInfo, which indicates the type of the context it contains.
     */
    public enum BlobInfoKind {
        /**
         *  Indicates that the context contains a path to the server file that represents this BLOB data.
         * In this case, the path can be obtained by calling getPath() method.
         */
        SERVER_PATH,

         /**
          * Indicates that the context contains a reference to the BLOB relay that represents this BLOB data.
          * In this case, the reference can be obtained by calling getBlobRelayReference() method.
          */
        BLOB_RELAY_REFERENCE,
    }

    /**
     * Creates a new instance of {@code ServerBlobInfo}.
     *
     * @param channelName the channel name for sending this BLOB data
     * @param serverPath the path of the server file that represents this BLOB data
     */
    public ServerBlobInfo(String channelName, String serverPath) {
        this.channelName = channelName;
        this.serverPath = serverPath;
        this.blobRelayReference = null;
        this.blobInfoKind = BlobInfoKind.SERVER_PATH;
    }

    /**
     * Creates a new instance of {@code ServerBlobInfo}.
     *
     * @param channelName the channel name for sending this BLOB data
     * @param blobRelayReference the reference to the BLOB relay that represents this BLOB data
     */
    public ServerBlobInfo(String channelName, BlobRelayReference blobRelayReference) {
        this.channelName = channelName;
        this.serverPath = null;
        this.blobRelayReference = blobRelayReference;
        this.blobInfoKind = BlobInfoKind.BLOB_RELAY_REFERENCE;
    }

    /**
     * Returns the kind of this ServerBlobInfo.
     * @return the kind of this ServerBlobInfo
     */
    public BlobInfoKind getBlobInfoKind() {
        return blobInfoKind;
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
     * @return the path of the server file
     * @throws IllegalStateException if this ServerBlobInfo does not contain a server path
     */
    public String getPath() {
        if (blobInfoKind != BlobInfoKind.SERVER_PATH) {
            throw new IllegalStateException("This ServerBlobInfo does not contain a server path");
        }
        return serverPath;
    }

    /**
     * Returns the reference to the BLOB relay that represents this BLOB data.
     * @return the reference to the BLOB relay
     * @throws IllegalStateException if this ServerBlobInfo does not contain a BlobRelayReference
     */
    public BlobRelayReference getBlobRelayReference() {
        if (blobInfoKind != BlobInfoKind.BLOB_RELAY_REFERENCE) {
            throw new IllegalStateException("This ServerBlobInfo does not contain a BlobRelayReference");
        }
        return blobRelayReference;
    }
}