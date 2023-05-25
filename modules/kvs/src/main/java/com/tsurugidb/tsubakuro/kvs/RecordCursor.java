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
