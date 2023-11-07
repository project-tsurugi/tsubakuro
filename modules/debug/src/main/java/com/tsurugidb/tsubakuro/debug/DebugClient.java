package com.tsurugidb.tsubakuro.debug;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A debugging service client.
 */
public interface DebugClient extends ServerResource {

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    static DebugClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        throw new UnsupportedOperationException();
    }

    /**
     * Requests to output a log record on the server side.
     * @param message the log message
     * @return the future response of the request
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> logging(@Nonnull String message) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests to output a log record on the server side.
     * @param level the log level
     * @param message the log message
     * @return the future response of the request
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<Void> logging(@Nonnull LogLevel level, @Nonnull String message) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Disposes the underlying server resources.
     * Note that, this never closes the underlying {@link Session}.
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
