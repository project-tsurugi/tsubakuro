package com.tsurugidb.tsubakuro.datastore.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

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

    private final long logBegin;

    private final long logEnd;

    private final Collection<BackupDetail.Entry> entries;

    /**
     * Creates a new instance.
     * @param configurationId the configuration ID
     * @param logBegin the start time of available transaction logs in this backup
     * @param logEnd the end time of available transaction logs in this backup
     * @param entries the next backup target files
     */
    public BackupDetailImpl(
            @Nonnull String configurationId,
            long logBegin, long logEnd,
            @Nonnull Collection<? extends BackupDetail.Entry> entries) {
        Objects.requireNonNull(configurationId);
        Objects.requireNonNull(entries);
        this.configurationId = configurationId;
        this.logBegin = logBegin;
        this.logEnd = logEnd;
        this.entries = List.copyOf(entries);
    }

    @Override
    public String getConfigurationId() {
        return configurationId;
    }

    @Override
    public long getLogBegin() {
        return logBegin;
    }

    @Override
    public long getLogEnd() {
        return logEnd;
    }

    @Override
    public Collection<? extends BackupDetail.Entry> nextEntries()
            throws IOException, ServerException, InterruptedException {
        return entries;
    }

    // FIXME impl

    @Override
    public String toString() {
        return String.format("BackupDetai(configurationId=%s, logBegin=%s, logEnd=%s)", //$NON-NLS-1$
                configurationId, logBegin, logEnd);
    }
}
