package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureWireImpl type.
 */
public class FutureWireImpl implements FutureResponse<Wire> {

    private IpcConnectorImpl connector;
    private long handle;
    private long id;
    private final AtomicBoolean done = new AtomicBoolean();

    FutureWireImpl(IpcConnectorImpl connector, long handle, long id) {
        this.connector = connector;
        this.handle = handle;
        this.id = id;
    }

    @Override
    public Wire get() throws IOException {
        done.set(true);
        return connector.getSessionWire(handle, id);
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws TimeoutException, IOException  {
        done.set(true);
        return connector.getSessionWire(handle, id, timeout, unit);
    }

    @Override
    public boolean isDone() {
        if (done.get()) {
            return true;
        }
        return connector.checkConnection(handle, id);
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        // FIXME: cancel connection if get() have never been called
    }
}
