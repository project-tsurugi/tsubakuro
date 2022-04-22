package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.util.List;
import java.util.LinkedList;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Path;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;

/**
 * Backup type.
 */
public class BackupImpl implements Backup {
    /**
     * Class constructor, called from  FutureBackupImpl.
     */
    public BackupImpl() {
    }

    /**
     * Get a list of file path
     * @return List of file path to be backuped
     */
    public List<Path> files() {
	List<Path> list = new LinkedList<>();
	list.add(Paths.get("/tmp", "backup-1"));
	return list;
    }

    /**
     * Close the Transaction
     */
    public void close() throws IOException {
    }
}