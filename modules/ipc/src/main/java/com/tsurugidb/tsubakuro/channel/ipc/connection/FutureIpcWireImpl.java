package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureIpcWireImpl type.
 */
public class FutureIpcWireImpl implements FutureResponse<Wire> {

    private final ClientInformation clientInformation;
    private IpcConnectorImpl connector;
    private long id;
    private final AtomicBoolean gotton = new AtomicBoolean();
    private final AtomicReference<Wire> result = new AtomicReference<>();
    private final boolean connectException;
    private boolean closed = false;

    FutureIpcWireImpl(IpcConnectorImpl connector, long id, @Nonnull ClientInformation clientInformation) {
        this.connector = connector;
        this.id = id;
        this.clientInformation = clientInformation;
        this.connectException = false;
    }

    FutureIpcWireImpl() {
        this.clientInformation = null;  // do not use when connectException occurs
        this.connectException = true;
    }

    private EndpointRequest.WireInformation wireInformation() {
        return EndpointRequest.WireInformation.newBuilder().setIpcInformation(
            EndpointRequest.WireInformation.IpcInformation.newBuilder().setConnectionInformation(Long.toString(ProcessHandle.current().pid()))
        ).build();
    }

    @Override
    public Wire get() throws IOException, ServerException, InterruptedException {
        if (connectException) {
            throw new ConnectException("the server has declined the connection request");
        }
        while (true) {
            var wire = result.get();
            if (wire != null) {
                return wire;
            }
            if (!gotton.getAndSet(true)) {
                WireImpl wireImpl = null;
                FutureResponse<Long> futureSessionID = null;
                try {
                    wireImpl = connector.getSessionWire(id);
                    futureSessionID = wireImpl.handshake(clientInformation, wireInformation());
                    wireImpl.checkSessionID(futureSessionID.get());
                    result.set(wireImpl);
                    return wireImpl;
                } catch (IOException | ServerException | InterruptedException e) {
                    closed = true;
                    try {
                        if (futureSessionID != null) {
                            futureSessionID.close();
                        }
                    } finally {
                        if (wireImpl != null) {
                            wireImpl.close();
                        }
                    }
                    throw e;
                }
            }
            if (closed) {
                throw new IOException("FutureIpcWireImpl is already closed");
            }
        }
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws TimeoutException, IOException, ServerException, InterruptedException {
        if (connectException) {
            throw new ConnectException("the server has declined the connection request");
        }
        while (true) {
            var wire = result.get();
            if (wire != null) {
                return wire;
            }
            if (!gotton.getAndSet(true)) {
                WireImpl wireImpl = null;
                FutureResponse<Long> futureSessionID = null;
                try {
                    wireImpl = connector.getSessionWire(id, timeout, unit);
                    futureSessionID = wireImpl.handshake(clientInformation, wireInformation());
                    wireImpl.checkSessionID(futureSessionID.get(timeout, unit));
                    result.set(wireImpl);
                    return wireImpl;
                } catch (TimeoutException | IOException | ServerException | InterruptedException e) {
                    closed = true;
                    try {
                        if (futureSessionID != null) {
                            futureSessionID.close();
                        }
                    } finally {
                        if (wireImpl != null) {
                            wireImpl.close();
                        }
                    }
                    throw e;
                }
            }
            if (closed) {
                throw new IOException("FutureIpcWireImpl is already closed");
            }
        }
    }

    @Override
    public boolean isDone() {
        if (result.get() != null) {
            return true;
        }
        return connector.checkConnection(id);
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            if (!closed) {
                closed = true;
                if (!connectException && result.get() == null) {
                    var wire = connector.getSessionWire(id);
                    wire.close();
                }
            }
        }
    }
}
