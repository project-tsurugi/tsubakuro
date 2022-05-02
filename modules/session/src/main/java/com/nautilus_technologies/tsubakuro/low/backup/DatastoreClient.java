package com.nautilus_technologies.tsubakuro.low.backup;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.backup.DatastoreClientImpl;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * A datastore service client.
 * @see #attach(Session)
 */
public interface DatastoreClient extends ServerResource {

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    static DatastoreClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return new DatastoreClientImpl(session);
    }

    /**
     * Starts backup.
     * @return the future response of started backup session
     * @throws IOException if I/O error was occurred while sending request
     * @throws InterruptedException if interrupted while sending request
     */
    default FutureResponse<Backup> beginBackup() throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    // FIXME more commands

    /**
     * Disposes the underlying server resources.
     * Note that, this never closes the underlying {@link Session}.
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}