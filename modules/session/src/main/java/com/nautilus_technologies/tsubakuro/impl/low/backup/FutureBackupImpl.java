package com.nautilus_technologies.tsubakuro.impl.low.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.LinkedList;
import java.util.List;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Lang;
import com.nautilus_technologies.tateyama.proto.DatastoreResponseProtos;
import com.nautilus_technologies.tsubakuro.channel.common.FutureInputStream;

/**
 * FutureBackupImpl type.
 */
public class FutureBackupImpl implements FutureResponse<Backup> {
    FutureInputStream futureInputStream;

    public FutureBackupImpl(FutureInputStream futureInputStream) {
        this.futureInputStream = futureInputStream;
    }

    @Override
    public Backup get() throws IOException, ServerException, InterruptedException {
        try {
            var response = DatastoreResponseProtos.BackupBegin.parseDelimitedFrom(futureInputStream.get());

            if (DatastoreResponseProtos.BackupBegin.ResultCase.SUCCESS.equals(response.getResultCase())) {
                List<Path> list = new LinkedList<>();
                var files = response.getSuccess().getFilesList();
                for (int i = 0; i < files.size(); i++) {
                    list.add(Path.of(files.get(i)));
                }
                return new BackupImpl(response.getSuccess().getId(), list);
            }
            throw new IOException(response.getUnknownError().getMessage());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: SessionWireImpl.receive()", e);
        }
    }

    @Override
    public Backup get(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        // FIXME: impl
        return get();
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
