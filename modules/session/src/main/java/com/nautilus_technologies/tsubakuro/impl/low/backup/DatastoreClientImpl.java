package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.low.backup.DatastoreClient;
import com.nautilus_technologies.tsubakuro.low.common.Session;

/**
 * An implementation of {@link DatastoreClient}.
 */
public class DatastoreClientImpl implements DatastoreClient {

    private final Session session;

    /**
     * Creates a new instance.
     * @param session the session to attach this client
     */
    public DatastoreClientImpl(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    // FIXME directly send request messages instead of delegate it via Session

    @Override
    public Future<Backup> beginBackup() throws IOException, InterruptedException {
        return session.beginBackup();
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
    }
}
