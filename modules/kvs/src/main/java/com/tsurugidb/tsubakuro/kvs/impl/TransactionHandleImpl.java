package com.tsurugidb.tsubakuro.kvs.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsTransaction;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

/**
 * An implementation of the {@link TransactionHandle}.
 */
public class TransactionHandleImpl implements TransactionHandle {

    private final KvsTransaction.Handle handle;
    private final KvsService service;
    private final ServerResourceHolder holder;

    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final AtomicBoolean commitOrRollbackCalled = new AtomicBoolean(false);

    /**
     * Creates a new instance.
     * @param systemId system Id of this handle got by KvsResponse.Begin
     * @param service KVS service to call Rollback or DisposeTransaction if necessary
     * @param holder handles {@link #close()} was invoked
     */
    public TransactionHandleImpl(long systemId, @Nullable KvsService service, @Nullable ServerResourceHolder holder) {
        var builder = KvsTransaction.Handle.newBuilder().setSystemId(systemId);
        this.handle = builder.build();
        this.holder = holder;
        if (holder != null) {
            holder.register(this);
        }
        this.service = service;
    }

    /**
     * Creates a new instance.
     * @param systemId system Id of this handle got by KvsResponse.Begin
     */
    public TransactionHandleImpl(long systemId) {
        this(systemId, null, null);
    }

    /**
     * retrieve the system id of this handle.
     * @return system id
     */
    public long getSystemId() {
        return handle.getSystemId();
    }

    KvsTransaction.Handle getHandle() {
        return handle;
    }

    boolean setDisposed() {
        return disposed.getAndSet(true);
    }

    boolean setCommitCalled() {
        return commitOrRollbackCalled.getAndSet(true);
    }

    boolean setRollbackCalled() {
        return setCommitCalled();
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        if (service == null) {
            return;
        }
        if (disposed.getAndSet(true)) {
            // Commit with AutoDispose=true succeeded or already disposed below
            return;
        }
        try {
            if (!commitOrRollbackCalled.getAndSet(true)) {
                var builder = KvsRequest.Rollback.newBuilder().setTransactionHandle(handle);
                service.send(builder.build()).await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (holder != null) {
                holder.onClosed(this);
            }
            var builder = KvsRequest.DisposeTransaction.newBuilder().setTransactionHandle(handle);
            service.send(builder.build()).await();
        }
    }

}
