/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.datastore.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.datastore.proto.DatastoreRequest;
import com.tsurugidb.tsubakuro.datastore.BackupDetail;
import com.tsurugidb.tsubakuro.datastore.DatastoreService;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * An implementation of {@link BackupDetail}.
 */
public class BackupDetailImpl implements BackupDetail {

    /**
     * An implementation of {@link com.tsurugidb.tsubakuro.datastore.BackupDetail.Entry BackupDetal.Entry}.
     */
    public static class Entry implements BackupDetail.Entry {

        private final Path sourcePath;

        private final Path destinationPath;

        private final boolean mutable;

        private final boolean detached;

        /**
         * Creates a new instance.
         * @param sourcePath the source path
         * @param destinationPath the destination path
         * @param mutable whether or not the target file can be modified after it was created
         * @param detached whether or not the target file detached from the database
         */
        public Entry(@Nonnull Path sourcePath, @Nonnull Path destinationPath, boolean mutable, boolean detached) {
            Objects.requireNonNull(sourcePath);
            Objects.requireNonNull(destinationPath);
            this.sourcePath = sourcePath;
            this.destinationPath = destinationPath;
            this.mutable = mutable;
            this.detached = detached;
        }

        @Override
        public Path getSourcePath() {
            return sourcePath;
        }

        @Override
        public Path getDestinationPath() {
            return destinationPath;
        }

        @Override
        public boolean isMutable() {
            return mutable;
        }

        @Override
        public boolean isDetached() {
            return detached;
        }

        @Override
        public String toString() {
            return String.format("Entry(sourcePath=%s, destinationPath=%s, mutable=%s, detached=%s)", //$NON-NLS-1$
                    sourcePath, destinationPath, mutable, detached);
        }
    }

    static final Logger LOG = LoggerFactory.getLogger(BackupDetailImpl.class);

    private final DatastoreService service;

    private final String configurationId;

    private final long logStart;

    private final long logFinish;

    private final @Nullable Long imageFinish;

    private final Collection<BackupDetail.Entry> entries;

    private final AtomicBoolean gotton = new AtomicBoolean();

    private final CloseHandler closeHandler;

    private Timeout closeTimeout = Timeout.DISABLED;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new instance.
     * @param configurationId the configuration ID
     * @param logBegin the start time of available transaction logs in this backup (64-bit unsigned integer)
     * @param logEnd the maximum time of available transaction logs in this backup (64-bit unsigned integer)
     * @param imageFinish the maximum time of available database image in this backup (64-bit unsigned integer)
     * @param entries the next backup target files
     */
    public BackupDetailImpl(
            @Nonnull String configurationId,
            long logBegin, long logEnd,
            @Nullable Long imageFinish,
            @Nonnull Collection<? extends BackupDetail.Entry> entries) {
        this(null, null, configurationId, logBegin, logEnd, imageFinish, entries);
    }

    /**
     * Creates a new instance.
     * @param service the backup service,
     *      this is required to tell end of backup to the server.
     *      If this is {@code null}, {@link #close()} will not do nothing
     * @param closeHandler handles {@link #close()} was invoked
     * @param configurationId the configuration ID
     * @param logBegin the start time of available transaction logs in this backup (64-bit unsigned integer)
     * @param logEnd the maximum time of available transaction logs in this backup (64-bit unsigned integer)
     * @param imageFinish the maximum time of available database image in this backup (64-bit unsigned integer)
     * @param entries the next backup target files
     */
    public BackupDetailImpl(
            @Nullable DatastoreService service,
            @Nullable ServerResource.CloseHandler closeHandler,
            @Nonnull String configurationId,
            long logBegin, long logEnd,
            @Nullable Long imageFinish,
            @Nonnull Collection<? extends BackupDetail.Entry> entries) {
        Objects.requireNonNull(configurationId);
        Objects.requireNonNull(entries);
        this.service = service;
        this.closeHandler = closeHandler;
        this.configurationId = configurationId;
        this.logStart = logBegin;
        this.logFinish = logEnd;
        this.imageFinish = imageFinish;
        this.entries = List.copyOf(entries);
    }

    @Override
    public String getConfigurationId() {
        return configurationId;
    }

    @Override
    public long getLogStart() {
        return logStart;
    }

    @Override
    public long getLogFinish() {
        return logFinish;
    }

    @Override
    public OptionalLong getImageFinish() {
        if (imageFinish == null) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(imageFinish);
    }

    @Override
    public Collection<? extends BackupDetail.Entry> nextEntries()
            throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            return entries;
        }
        return null;
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
        if (service == null || closed.get()) {
            return;
        }
        try (var response = service.updateExpirationTime(timeout, unit)) {
            // FIXME: don't wait, delay the receive response until next keepAlive() or close()
            response.get();
        }
    }

    @Override
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
        if (closeHandler != null) {
            Lang.suppress(
                    e -> LOG.warn("error occurred while collecting garbage", e),
                    () -> closeHandler.onClosed(this));
        }
        if (service != null) {
            try {
                var request = DatastoreRequest.BackupEnd.newBuilder()
                    .setId(Long.parseLong(configurationId))
                    .build();
                var response = service.send(request);
                closeTimeout.waitFor(response);
            } catch (NumberFormatException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("BackupDetai(configurationId=%s, image=%s, log=[%s, %s])", //$NON-NLS-1$
                configurationId,
                Optional.ofNullable(imageFinish)
                        .map(it -> String.format("[, %s]", it)) //$NON-NLS-1$
                        .orElse("N/A"), //$NON-NLS-1$
                logStart, logFinish);
    }
}
