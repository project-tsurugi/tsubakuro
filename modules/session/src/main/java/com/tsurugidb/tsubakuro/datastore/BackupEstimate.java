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
package com.tsurugidb.tsubakuro.datastore;

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
