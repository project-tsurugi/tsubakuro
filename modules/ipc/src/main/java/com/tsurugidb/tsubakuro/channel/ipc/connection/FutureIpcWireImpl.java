package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureIpcWireImpl type.
 */
public class FutureIpcWireImpl implements FutureResponse<Wire> {

    private IpcConnectorImpl connector;
    private long id;
    private final AtomicBoolean gotton = new AtomicBoolean();

    FutureIpcWireImpl(IpcConnectorImpl connector, long id) {
        this.connector = connector;
        this.id = id;
    }

    @Override
    public Wire get() throws IOException {
        if (!gotton.getAndSet(true)) {
            return connector.getSessionWire(id);
        }
        throw new IOException("FutureIpcWireImpl is already closed");
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws TimeoutException, IOException  {
        if (!gotton.getAndSet(true)) {
            return connector.getSessionWire(id, timeout, unit);
        }
        throw new IOException("FutureIpcWireImpl is already closed");
    }

    @Override
    public boolean isDone() {
        if (gotton.get()) {
            return true;
        }
        return connector.checkConnection(id);
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            var wire = connector.getSessionWire(id);
            wire.close();
        }
    }
}
