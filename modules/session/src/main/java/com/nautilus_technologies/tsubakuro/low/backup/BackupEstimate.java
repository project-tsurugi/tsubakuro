package com.nautilus_technologies.tsubakuro.low.backup;

import java.text.MessageFormat;
import java.util.Objects;

/**
 * Estimated information of backup operation.
 */
public class BackupEstimate {

    private final long numberOfFiles;

    private final long numberOfBytes;

    /**
     * Creates a new instance.
     * @param numberOfFiles the estimated number of files
     * @param numberOfBytes the estimated number of bytes
     */
    public BackupEstimate(long numberOfFiles, long numberOfBytes) {
        this.numberOfFiles = numberOfFiles;
        this.numberOfBytes = numberOfBytes;
    }

    /**
     * Returns the estimated number of files to copy.
     * @return the estimated files
     */
    public long getNumberOfFiles() {
        return numberOfFiles;
    }

    /**
     * Returns the estimated number of bytes to copy.
     * @return the estimated bytes
     */
    public long getNumberOfBytes() {
        return numberOfBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfBytes, numberOfFiles);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BackupEstimate other = (BackupEstimate) obj;
        return numberOfBytes == other.numberOfBytes && numberOfFiles == other.numberOfFiles;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "BackupEstimate(numberOfFiles={0}, numberOfBytes={1})",
                numberOfFiles,
                numberOfBytes);
    }
}
