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
package com.tsurugidb.tsubakuro.kvs;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A cursor for retrieving contents in {@code SCAN} results.
 */
@NotThreadSafe
public interface RecordCursor extends ServerResource {

    /**
     * Advances the cursor to the head of the next record.
     * <p>
     * If this operation was succeeded (returns {@code true}), this cursor points the head of the next record.
     * After this operation, you can invoke {@link #getRecord()} to retrieve the record on the cursor position.
     * </p>
     * @return {@code true} if the cursor successfully advanced to the head of the next record,
     *  or {@code false} if there are no more records in this cursor
     * @throws IOException if I/O error was occurred while retrieving the next record
     * @throws ServerException if server error was occurred while retrieving records
     * @throws InterruptedException if interrupted while retrieving the next record
     */
    boolean next() throws IOException, ServerException, InterruptedException;

    /**
     * Returns the record on this cursor position.
     * @return the record on this cursor position
     * @throws IllegalStateException if this cursor does not point any records
     * @throws IOException if I/O error was occurred while extracting the record
     * @throws ServerException if server error was occurred while retrieving the record
     * @throws InterruptedException if interrupted while extracting the record
     */
    Record getRecord() throws IOException, ServerException, InterruptedException;
}
