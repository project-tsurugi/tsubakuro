package com.nautilus_technologies.tsubakuro.impl.low.datastore;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.tsurugidb.tateyama.proto.DatastoreRequest;
import com.nautilus_technologies.tsubakuro.low.datastore.Backup;
import com.nautilus_technologies.tsubakuro.low.datastore.BackupEstimate;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreClient;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreService;
import com.nautilus_technologies.tsubakuro.low.datastore.Tag;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link DatastoreClient}.
 */
@ThreadSafe
public class DatastoreClientImpl implements DatastoreClient {

    private final DatastoreService service;

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    public static DatastoreClientImpl attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return new DatastoreClientImpl(new DatastoreServiceStub(session));
    }

    /**
     * Creates a new instance.
     * @param service the service stub
     */
    public DatastoreClientImpl(@Nonnull DatastoreService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public FutureResponse<Backup> beginBackup(@Nullable String label) throws IOException {
        var builder = DatastoreRequest.BackupBegin.newBuilder();
        if (label != null) {
            builder.setLabel(label);
        }
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<BackupEstimate> estimateBackup() throws IOException {
        var builder = DatastoreRequest.BackupEstimate.newBuilder();
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<List<Tag>> listTag() throws IOException {
        var builder = DatastoreRequest.TagList.newBuilder();
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Optional<Tag>> getTag(@Nonnull String name) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequest.TagGet.newBuilder()
                .setName(name);
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Tag> addTag(@Nonnull String name, @Nullable String comment) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequest.TagAdd.newBuilder()
                .setName(name);
        if (!Objects.isNull(comment)) {
            builder.setComment(comment);
        }
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Boolean> removeTag(@Nonnull String name) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequest.TagRemove.newBuilder()
                .setName(name);
        return service.send(builder.build());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        service.close();
    }
}
