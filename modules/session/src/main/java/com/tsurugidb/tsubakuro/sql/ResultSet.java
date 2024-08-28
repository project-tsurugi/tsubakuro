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
package com.tsurugidb.tsubakuro.sql;

import java.io.IOException;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Represents a server side SQL result set.
 */
public interface ResultSet extends RelationCursor {

    /**
     * Returns the metadata of this result set.
     * @return the metadata
     * @throws IOException if I/O error was occurred while retrieving metadata
     * @throws ServerException if server error was occurred during underlying operation
     * @throws InterruptedException if interrupted while retrieving metadata
     */
    ResultSetMetadata getMetadata() throws IOException, ServerException, InterruptedException;

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
