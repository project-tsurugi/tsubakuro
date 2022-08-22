package com.nautilus_technologies.tsubakuro.impl.low.datastore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tateyama.proto.DatastoreRequest;
import com.nautilus_technologies.tsubakuro.low.datastore.Backup;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreService;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.Lang;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tsubakuro.util.Timeout;

/**
 * An implementation of {@link Backup}.
 */
public class BackupImpl implements Backup {

    static final Logger LOG = LoggerFactory.getLogger(BackupImpl.class);

    private static final long MAX_EXPIRATION_TIME_MILLIS = TimeUnit.HOURS.toMillis(24);

    private final DatastoreService service;

    private final long backupId;

    private final List<Path> files;

    private final CloseHandler closeHandler;

    private Timeout closeTimeout = Timeout.DISABLED;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new instance.
     * @param files the files to copy
     */
    public BackupImpl(@Nonnull Collection<? extends Path> files) {
        this(null, null, 0, files);
    }

    /**
     * Creates a new instance.
     * @param files the files to copy
     */
     public BackupImpl(long backupId, @Nonnull Collection<? extends Path> files) {
        this(null, null, backupId, files);
    }

    /**
     * Creates a new instance.
     * @param service the backup service,
     *      this is required to tell end of backup to the server.
     *      If this is {@code null}, {@link #close()} will not do nothing
     * @param closeHandler handles {@link #close()} was invoked
     * @param backupId the backup operation ID
     * @param files the files to copy
     */
    public BackupImpl(
            @Nullable DatastoreService service,
            @Nullable ServerResource.CloseHandler closeHandler,
            long backupId,
            @Nonnull Collection<? extends Path> files) {
        Objects.requireNonNull(files);
        this.service = service;
        this.closeHandler = closeHandler;
        this.backupId = backupId;
        this.files = List.copyOf(files);
    }

    /**
     * Returns the file paths to copy.
     * @return file paths
     */
    @Override
    public List<Path> getFiles() {
        return files;
    }

    /**
     * Tries to extend the expiration time of the current backup operation.
     * @param timeout the expiration time
     * @param unit time unit of the expiration time
     * @throws IOException if I/O error was occurred while requesting to the service
     * @throws ServerException if the request was failed
     * @throws InterruptedException if interrupted while requesting
     */
    @Override
    public void keepAlive(int timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException {
        if (Objects.isNull(service) || closed.get()) {
            return;
        }
        try (var response = service.updateExpirationTime(timeout, unit)) {
            // FIXME: don't wait, delay the receive response until next keepAlive() or close()
            response.get();
        }
    }

    public void setCloseTimeout(@Nonnull Timeout timeout) {
        Objects.requireNonNull(timeout);
        closeTimeout = timeout;
    }

    /**
     * Tells the backup operation was completed to the service.
     */
    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (closed.get()) {
            return;
        }
        closed.set(true);
        if (Objects.nonNull(closeHandler)) {
            Lang.suppress(
                    e -> LOG.warn("error occurred while collecting garbage", e),
                    () -> closeHandler.onClosed(this));
        }
        if (Objects.nonNull(service)) {
            var request = DatastoreRequest.BackupEnd.newBuilder()
                    .setId(backupId)
                    .build();
            var response = service.send(request);
            closeTimeout.waitFor(response);
        }
    }
}
