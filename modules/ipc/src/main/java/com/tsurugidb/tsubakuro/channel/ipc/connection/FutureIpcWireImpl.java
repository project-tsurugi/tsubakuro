package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureIpcWireImpl type.
 */
public class FutureIpcWireImpl implements FutureResponse<Wire> {

    private final Credential credential;
    private final ClientInformation clientInformation;
    private IpcConnectorImpl connector;
    private long id;
    private final AtomicBoolean gotton = new AtomicBoolean();

    FutureIpcWireImpl(IpcConnectorImpl connector, long id, @Nonnull Credential credential, @Nonnull ClientInformation clientInformation) {
        this.connector = connector;
        this.id = id;
        this.credential = credential;
        this.clientInformation = clientInformation;
    }

    @Override
    public Wire get() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            var wire = connector.getSessionWire(id);
            if (wire instanceof WireImpl) {
                var wireImpl = (WireImpl) wire;
                var futureSessionID = wireImpl.handshake(credential, clientInformation);
                wireImpl.checkSessionID(futureSessionID.get());
                return wireImpl;
            }
            throw new IOException("FutureIpcWireImpl programing error");  // never occure
        }
        throw new IOException("FutureIpcWireImpl is already closed");
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws TimeoutException, IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            var wire = connector.getSessionWire(id, timeout, unit);
            if (wire instanceof WireImpl) {
                var wireImpl = (WireImpl) wire;
                var futureSessionID = wireImpl.handshake(credential, clientInformation);
                wireImpl.checkSessionID(futureSessionID.get(timeout, unit));
                return wireImpl;
            }
            throw new IOException("FutureIpcWireImpl programing error");  // never occure
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
