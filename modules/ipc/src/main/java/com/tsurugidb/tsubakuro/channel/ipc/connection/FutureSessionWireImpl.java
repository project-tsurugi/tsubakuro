package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureSessionWireImpl type.
 */
public class FutureSessionWireImpl implements FutureResponse<Wire> {

    IpcConnectorImpl connector;

    FutureSessionWireImpl(IpcConnectorImpl connector) {
        this.connector = connector;
    }

    @Override
    public Wire get() throws IOException {
        return connector.getSessionWire();
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws TimeoutException, IOException  {
        return connector.getSessionWire(timeout, unit);
    }

    @Override
    public boolean isDone() {
        return connector.checkConnection();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        // FIXME: cancel connection if get() have never been called
    }
}
