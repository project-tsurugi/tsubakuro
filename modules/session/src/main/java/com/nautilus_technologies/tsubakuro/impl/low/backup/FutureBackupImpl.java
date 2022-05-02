package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Lang;

/**
 * FutureBackupImpl type.
 */
public class FutureBackupImpl implements FutureResponse<Backup> {

    @Override
    public Backup get() throws IOException, ServerException, InterruptedException {
        // FIXME: impl
        return new BackupImpl();
    }

    @Override
    public Backup get(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        // FIXME: impl
        return new BackupImpl();
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        // FIXME: impl
        Lang.pass();
    }
}
