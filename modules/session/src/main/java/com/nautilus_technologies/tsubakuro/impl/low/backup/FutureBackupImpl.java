package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;

/**
 * FutureBackupImpl type.
 */
public class FutureBackupImpl implements Future<Backup> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     */
    public FutureBackupImpl() {
    }

    public BackupImpl get() throws ExecutionException {
	return new BackupImpl();
    }

    public BackupImpl get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	return new BackupImpl();
    }
    public boolean isDone() {
	return isDone;
    }
    public boolean isCancelled() {
	return isCancelled;
    }
    public boolean cancel(boolean mayInterruptIfRunning) {
	isCancelled = true;
	isDone = true;
	return true;
    }
}
