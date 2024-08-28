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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * Represents detail backup information.
 *
 * <p><b>example snippet</b></p>
<pre>{@code Session session = ...;
Path destination = ...;
try (
    var client = DatastoreClient.attach(session);
    var backup = client.beginBackup(BackupType.STANDARD).await();
) {
    while (true) {
        // fetch next entries
        var entries = backup.nextEntries();
        if (entries == null) {
            break;
        }

        for (var entry : entries) {
            var from = entry.getSourcePath();
            var to = destination.resolve(entry.getDestinationPath());
            if (entry.isDetached()) {
                // ... move file
            } else {
                // ... copy file
            }
        }
        backup.keepAlive();
    }
}
}</pre>
 * @see Backup
 * @see DatastoreClient#beginBackup(BackupType)
 */
public interface BackupDetail extends ServerResource {

    /**
     * Represents a backup target file.
     */
    interface Entry {

        /**
         * Returns the the source file of the backup target.
         * @return the source file (absolute path)
         */
        Path getSourcePath();

        /**
         * Returns the deployment path of the backup target.
         * <p>
         * If two backup operations have files with the same deployment path,
         * you can consider they are the same file.
         * </p>
         * @return the deployment path (relative path)
         * @see BackupDetail#getConfigurationId()
         */
        Path getDestinationPath();

        /**
         * Returns whether or not the target file can be modified after it was created.
         * @return {@code false} if the file is immutable, or {@code true} otherwise
         * @see #getDestinationPath()
         */
        default boolean isMutable() {
            return false;
        }

        /**
         * Returns whether or not the target file detached from the database.
         * <p>
         * If the file is detached, you can move it to take a backup.
         * Or it is not detached, you must copy it.
         * </p>
         * @return {@code false} if the file is immutable, or {@code true} otherwise
         * @see #getDestinationPath()
         */
        default boolean isDetached() {
            return false;
        }
    }

    /**
     * Returns the configuration ID of this backup.
     * <p>
     * If two backup operations have the same configuration ID,
     * files with the same {@link Entry#getDestinationPath() deployment path} can be
     * considered with the same file.
     * </p>
     * <p>
     * Ordinary, the datastore will decide the configuration ID using the following elements:
     * </p>
     * <ul>
     * <li> data layout in file </li>
     * <li> path layout on file system </li>
     * </ul>
     * @return the configuration ID
     */
    String getConfigurationId();

    /**
     * Returns the start time of available transaction logs in this backup.
     * <p>
     * This "time" has nothing to do with the real clock, so that you cannot retrieve the actual time from it.
     * </p>
     * <p>
     * The returned value is an unsigned integer.
     * To compare two values, you must use {@link Long#compareUnsigned(long, long)}.
     * </p>
     * @return the start time of available transaction logs (64-bit unsigned integer)
     * @see #getLogFinish()
     */
    long getLogStart();

    /**
     * Returns the maximum time of available transaction logs in this backup.
     * <p>
     * This "time" has nothing to do with the real clock, so that you cannot retrieve the actual time from it.
     * </p>
     * <p>
     * The returned value is an unsigned integer.
     * To compare two values, you must use {@link Long#compareUnsigned(long, long)}.
     * </p>
     * @return the maximum time of available transaction logs (64-bit unsigned integer)
     * @see #getLogStart()
     */
    long getLogFinish();

    /**
     * Returns the maximum time of available database image in this backup.
     * <p>
     * This "time" has nothing to do with the real clock, so that you cannot retrieve the actual time from it.
     * </p>
     * <p>
     * The returned value is an unsigned integer.
     * To compare two values, you must use {@link Long#compareUnsigned(long, long)}.
     * </p>
     * @return the maximum time of available database image (64-bit unsigned integer)
     *      or {@code empty} if database image is not available in this backup target
     * @see #getLogStart()
     */
    OptionalLong getImageFinish();

    /**
     * Retrieves the next backup target files.
     * @return the next entries, or {@code null} if this backup does not have no more entries
     * @throws IOException if I/O error was occurred while requesting to the service
     * @throws ServerException if the request was failed
     * @throws InterruptedException if interrupted while requesting
     */
    @Nullable Collection<? extends Entry> nextEntries() throws IOException, ServerException, InterruptedException;

    /**
     * Tries to extend the expiration time of the current backup operation.
     * @param timeout the expiration time
     * @param unit time unit of the expiration time
     * @throws IOException if I/O error was occurred while requesting to the service
     * @throws ServerException if the request was failed
     * @throws InterruptedException if interrupted while requesting
     */
    default void keepAlive(int timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException {
        // do nothing
    }

    /**
     * Tells the backup operation was completed to the service.
     */
    @Override
    default void close() throws IOException, ServerException, InterruptedException {
        // do nothing
    }
}
