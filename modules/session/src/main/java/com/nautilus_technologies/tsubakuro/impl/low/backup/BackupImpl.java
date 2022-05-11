package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import com.nautilus_technologies.tsubakuro.low.backup.Backup;

/**
 * Backup type.
 */
public class BackupImpl implements Backup {
    long backupID;
    
    /**
     * Class constructor, called from  FutureBackupImpl.
     */
    public BackupImpl() {
    }
    public BackupImpl(long backupID) {
        this.backupID = backupID;
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
     * Get the the backup ID
     * @return backup ID
     */
    public long backupID() {
        return backupID;
    }
    
    /**
     * Close the Transaction
     */
    public void close() throws IOException {
    }
}
