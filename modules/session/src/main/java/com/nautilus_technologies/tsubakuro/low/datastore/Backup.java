package com.nautilus_technologies.tsubakuro.low.datastore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * Represents backup information.
 * <p>
 * This only provides paths of individual backup target files.
 * To take a backup, you must copy them to some directory.
 * Please take care to keep each file name during copy,
 * because the database will identify individual backed-up files from their file names.
 * Note that, you can change directory hierarchy of individual files.
 * </p>
 *
 * <h3>example snippet</h3>
<pre>
Path destination = Paths.get("/path/to/backup-target");
try (Backup backup = ...) {
    for (Path source : backup) {
        // extend the expiration time
        backup.keepAlive(5, TimeUnit.MINUTE);

        // copies file without changing its file name
        Files.copy(source, destination.resolve(source.getFileName()))
    }
}
</pre>
 * @see DatastoreClient#beginBackup()
 */
public interface Backup extends ServerResource, Iterable<Path> {

    /**
     * Returns the file paths to copy.
     * @return file paths
     */
    @Override
    default Iterator<Path> iterator() {
        return getFiles().iterator();
    }

    /**
     * Returns the file paths to copy.
     * @return file paths
     */
    List<Path> getFiles();

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