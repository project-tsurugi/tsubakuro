package com.nautilus_technologies.tsubakuro.low.backup;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Backup type.
 */
public interface Backup extends Closeable {
    /**
     * Get a list of file path
     * @return List of file path to be backuped
     * @throws IOException error occurred in listing backup files
     */
    Collection<? extends Path> files() throws IOException;
}
