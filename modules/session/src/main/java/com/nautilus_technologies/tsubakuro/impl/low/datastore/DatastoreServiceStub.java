package com.nautilus_technologies.tsubakuro.impl.low.datastore;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
// import java.nio.ByteBuffer;
// import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.Instant;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.List;
import java.util.Objects;
// import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tateyama.proto.DatastoreCommonProtos;
import com.nautilus_technologies.tateyama.proto.DatastoreRequestProtos;
import com.nautilus_technologies.tateyama.proto.DatastoreResponseProtos;
import com.nautilus_technologies.tsubakuro.low.datastore.Backup;
// import com.nautilus_technologies.tsubakuro.low.datastore.BackupEstimate;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreService;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreServiceCode;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreServiceException;
import com.nautilus_technologies.tsubakuro.low.datastore.Tag;
import com.nautilus_technologies.tsubakuro.exception.BrokenResponseException;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResourceHolder;
import com.google.protobuf.Message;

/**
 * An implementation of {@link DatastoreService} communicate to the datastore service.
 */
public class DatastoreServiceStub implements DatastoreService {

    static final Logger LOG = LoggerFactory.getLogger(DatastoreServiceStub.class);

    /**
     * The datastore service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_BACKUP;

    private final Session session;

    private final ServerResourceHolder resources = new ServerResourceHolder();

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public DatastoreServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    static DatastoreServiceException newUnknown(@Nonnull DatastoreResponseProtos.UnknownError message) {
        assert message != null;
        return new DatastoreServiceException(DatastoreServiceCode.UNKNOWN, message.getMessage());
    }

    static BrokenResponseException newResultNotSet(
            @Nonnull Class<? extends Message> aClass, @Nonnull String name) {
        assert aClass != null;
        assert name != null;
        return new BrokenResponseException(MessageFormat.format(
                "{0}.{1} is not set",
                aClass.getSimpleName(),
                name));
    }

    static Tag convert(@Nonnull DatastoreCommonProtos.Tag tag) {
        assert tag != null;
        return new Tag(
                tag.getName(),
                optional(tag.getComment()),
                optional(tag.getAuthor()),
                Instant.ofEpochMilli(tag.getTimestamp()));
    }

    private static @Nullable String optional(@Nullable String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return value;
    }

    @Override
    public FutureResponse<Backup> send(@Nonnull DatastoreRequestProtos.BackupBegin request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return new FutureBackupImpl(session.send(Constants.SERVICE_ID_BACKUP, toByteArray(DatastoreRequestProtos.Request.newBuilder()
                                                                                          .setMessageVersion(Constants.MESSAGE_VERSION)
                                                                                          .setBackupBegin(request)
                                                                                          .build())));
    }

// FIXME user response processor
    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        LOG.trace("closing underlying resources"); //$NON-NLS-1$
        resources.close();
    }

// FIXME should process at transport layer
    private byte[] toByteArray(DatastoreRequestProtos.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
}
