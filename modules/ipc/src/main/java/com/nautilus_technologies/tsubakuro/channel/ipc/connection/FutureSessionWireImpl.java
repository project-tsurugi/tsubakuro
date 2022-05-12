package com.nautilus_technologies.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * FutureSessionWireImpl type.
 */
public class FutureSessionWireImpl implements FutureResponse<SessionWire> {

    IpcConnectorImpl connector;

    FutureSessionWireImpl(IpcConnectorImpl connector) {
        this.connector = connector;
    }

    @Override
    public SessionWire get() throws IOException {
        return connector.getSessionWire();
    }

    @Override
    public SessionWire get(long timeout, TimeUnit unit) throws TimeoutException, IOException  {
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
