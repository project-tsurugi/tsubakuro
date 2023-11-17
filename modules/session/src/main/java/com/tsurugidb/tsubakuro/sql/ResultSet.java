package com.tsurugidb.tsubakuro.sql;

import java.io.IOException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

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

    /**
     * Get a FutureResponse of the response returned from the SQL service
     * @return a FutureResponse of SqlResponse.ResultOnly indicate whether the SQL service has successfully completed processing or not
     * @deprecated {@code FutureResponse<Void>} is checked at next() and/or close(), thus it is unnecessary to provide {@code FutureResponse<Void>} to tsubakuro's clinets
     */
    @Deprecated
    default FutureResponse<Void> getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
