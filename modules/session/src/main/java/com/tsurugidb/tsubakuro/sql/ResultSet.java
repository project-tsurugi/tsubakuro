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
