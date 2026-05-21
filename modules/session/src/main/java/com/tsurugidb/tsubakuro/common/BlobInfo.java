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

import java.nio.file.Path;
import java.util.Optional;

/**
 * An abstract super interface of BLOB data to send to Tsurugi server.
 *
 * @since 1.8.0
 */
public interface BlobInfo {

    /**
     * Returns the channel name for sending this BLOB data.
     * <p>
     * The channel name is used to identify the BLOB data in the server side,
     * so that it must be unique in the same request.
     * </p>
     * @return the channel name
     */
    default String getChannelName() {
        throw new UnsupportedOperationException("getChannelName is not implemented");
    }

    /**
     * Returns whether there is a file that represent this BLOB data in the local file system.
     * @return {@code true} if there is a file, otherwise {@code false}
     * @see #getPath()
     */
    default boolean isFile() {
        throw new UnsupportedOperationException("isFile is not implemented");
    }

    /**
     * Returns the path of the file that represents this BLOB data, only if it exists.
     * @return the path of the file, or empty if it does not exist
     * @see #isFile()
     */
    default Optional<Path> getPath() {
        throw new UnsupportedOperationException("getPath is not implemented");
    }

    /**
     * Returns the server path of the LargeObject uploaded to the BLOB relay service.
     * <p>
     * Used when uploading BLOBs via the BLOB relay service.
     * </p>
     * @return the server path of the LargeObject, or empty when the BLOB relay service is not used.
     *
     * @since 1.11.0
     */
    default Optional<String> getServerPath() {
        throw new UnsupportedOperationException();
    };

    /**
     * Returns the BlobRelayReference of the LargeObject uploaded to the BLOB relay service.
     * <p>
     * Used when uploading BLOBs via the BLOB relay service.
     * </p>
     * @return the BlobRelayReference of the LargeObject, or empty when the BLOB relay service is not used.
     *
     * @since 1.11.0
     */
    default Optional<BlobRelayReference> getBlobRelayReference() {
        throw new UnsupportedOperationException();
    };
}