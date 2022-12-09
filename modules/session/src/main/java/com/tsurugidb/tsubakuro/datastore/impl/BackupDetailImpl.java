package com.tsurugidb.tsubakuro.datastore.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.datastore.BackupDetail;
import com.tsurugidb.tsubakuro.exception.ServerException;

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

    private final String configurationId;

    private final long logStart;

    private final long logFinish;

    private final @Nullable Long imageFinish;

    private final Collection<BackupDetail.Entry> entries;

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
        Objects.requireNonNull(configurationId);
        Objects.requireNonNull(entries);
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
        return entries;
    }

    // FIXME impl

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
