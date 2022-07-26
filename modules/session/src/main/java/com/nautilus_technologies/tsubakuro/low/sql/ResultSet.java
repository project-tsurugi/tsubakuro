package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.jogasaki.proto.SqlResponse;

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
     */
    FutureResponse<SqlResponse.ResultOnly> getResponse();

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
