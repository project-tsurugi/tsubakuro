package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tateyama.proto.DatastoreRequestProtos;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.low.backup.BackupEstimate;
import com.nautilus_technologies.tsubakuro.low.backup.DatastoreClient;
import com.nautilus_technologies.tsubakuro.low.backup.DatastoreService;
import com.nautilus_technologies.tsubakuro.low.backup.Tag;
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
    public FutureResponse<Backup> beginBackup() throws IOException {
        var builder = DatastoreRequestProtos.BackupBegin.newBuilder();
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<BackupEstimate> estimateBackup() throws IOException {
        var builder = DatastoreRequestProtos.BackupEstimate.newBuilder();
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<List<Tag>> listTag() throws IOException {
        var builder = DatastoreRequestProtos.TagList.newBuilder();
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Optional<Tag>> getTag(@Nonnull String name) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequestProtos.TagGet.newBuilder()
                .setName(name);
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Tag> addTag(@Nonnull String name, @Nullable String comment) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequestProtos.TagAdd.newBuilder()
                .setName(name);
        if (!Objects.isNull(comment)) {
            builder.setComment(comment);
        }
    	return service.send(builder.build());
    }

    @Override
    public FutureResponse<Boolean> removeTag(@Nonnull String name) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequestProtos.TagRemove.newBuilder()
                .setName(name);
        return service.send(builder.build());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        service.close();
    }
}
