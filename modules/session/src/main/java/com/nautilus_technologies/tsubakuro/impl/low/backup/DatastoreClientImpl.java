package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.low.backup.DatastoreClient;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tateyama.proto.DatastoreRequestProtos;

/**
 * An implementation of {@link DatastoreClient}.
 */
public class DatastoreClientImpl implements DatastoreClient {
    static final long MESSAGE_VERSION = 1;
    static final long SERVICE_ID_DATASTORE = 2;

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
    public FutureResponse<Backup> beginBackup() throws IOException, InterruptedException {
        try (var buffer = new ByteArrayOutputStream()) {
            DatastoreRequestProtos.Request.newBuilder()
            .setMessageVersion(MESSAGE_VERSION)
            .setBackupBegin(DatastoreRequestProtos.BackupBegin.newBuilder())
            .build().writeDelimitedTo(buffer);
            return new FutureBackupImpl(session.send(SERVICE_ID_DATASTORE, buffer.toByteArray()));
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
    }
}
