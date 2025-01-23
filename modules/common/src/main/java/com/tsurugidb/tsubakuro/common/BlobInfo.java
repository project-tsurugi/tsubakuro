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
package com.tsurugidb.tsubakuro.common;

import java.nio.file.Path;
import java.util.Optional;

/**
 * An abstract super interface of BLOB data to send to Tsurugi server.
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
    String getChannelName();

    /**
     * Returns whether there is a file that represent this BLOB data in the local file system.
     * @return {@code true} if there is a file, otherwise {@code false}
     * @see #getPath()
     */
    boolean isFile();

    /**
     * Returns the path of the file that represents this BLOB data, only if it exists.
     * @return the path of the file, or empty if it does not exist
     * @see #isFile()
     */
    Optional<Path> getPath();
}