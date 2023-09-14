package com.tsurugidb.tsubakuro.kvs.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsTransaction;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

/**
 * An implementation of the {@link TransactionHandle}.
 */
public class TransactionHandleImpl implements TransactionHandle {

    static final Logger LOG = LoggerFactory.getLogger(TransactionHandleImpl.class);

    private final KvsTransaction.Handle handle;
    private final KvsService service;
    private final ServerResourceHolder holder;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean commitAutoDisposed = new AtomicBoolean(false);
    private final AtomicBoolean commitOrRollbackCalled = new AtomicBoolean(false);

    /**
     * Creates a new instance.
     * @param systemId system Id of this handle got by KvsResponse.Begin
     * @param service KVS service to call ROLLBACK or DisposeTransaction if necessary at {@link #close()}
     * @param holder handles {@link #close()} was invoked
     */
    public TransactionHandleImpl(long systemId, @Nullable KvsService service, @Nullable ServerResourceHolder holder) {
        var builder = KvsTransaction.Handle.newBuilder().setSystemId(systemId);
        this.handle = builder.build();
        this.holder = holder;
        this.service = service;
        if (holder != null) {
            holder.register(this);
        }
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

    boolean setCommitAutoDisposed() {
        // COMMIT succeeded with autoDispose=true
        // At close(), calling ROLLBACK and DisposeTx is unnecessary
        return commitAutoDisposed.getAndSet(true);
    }

    boolean setCommitCalled() {
        // COMMIT called, it maybe succeed or fail
        // At close(), calling ROLLBACK is unnecessary, DisposeTx is necessary
        return commitOrRollbackCalled.getAndSet(true);
    }

    boolean clearCommitCalled() {
        // TODO delete this method when commit fully works
        // COMMIT called, but failed with NOT_IMPLEMENTED
        // At close(), calling ROLLBACK and DisposeTx is necessary
        return commitOrRollbackCalled.getAndSet(false);
    }

    boolean setRollbackCalled() {
        // ROLLBACK called, it maybe succeed or fail
        // At close(), calling ROLLBACK is unnecessary, DisposeTx is necessary
        return setCommitCalled();
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        if (closed.getAndSet(true)) {
            return;
        }
        if (holder != null) {
            holder.onClosed(this);
        }
        if (service == null || commitAutoDisposed.get()) {
            return;
        }
        try {
            if (!commitOrRollbackCalled.getAndSet(true)) {
                var builder = KvsRequest.Rollback.newBuilder().setTransactionHandle(handle);
                service.send(builder.build()).await();
            }
        } catch (Exception e) {
            LOG.warn("rollback failed during transaction handle closing", e);
        } finally {
            var builder = KvsRequest.DisposeTransaction.newBuilder().setTransactionHandle(handle);
            service.send(builder.build()).await();
        }
    }

}
