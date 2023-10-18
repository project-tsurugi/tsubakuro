package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.ipc.NativeLibrary;
import com.tsurugidb.tsubakuro.channel.ipc.IpcLink;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * IpcConnectorImpl type.
 */
public final class IpcConnectorImpl implements Connector {

    private static final Logger LOG = LoggerFactory.getLogger(IpcConnectorImpl.class);

    private static native long getConnectorNative(String name) throws IOException;
    private static native long requestNative(long handle) throws IOException;
    private static native long waitNative(long handle, long id);
    private static native long waitNative(long handle, long id, long timeout) throws TimeoutException;
    private static native boolean checkNative(long handle, long id);
    private static native void closeConnectorNative(long handle);

    private final String name;

    static {
        NativeLibrary.load();
    }

    public IpcConnectorImpl(String name) {
        this.name = name;
    }

    @Override
    public FutureResponse<Wire> connect(Credential credential) throws IOException {
        LOG.trace("will connect to {}", name); //$NON-NLS-1$

        long handle = getConnectorNative(name);
        try {
            long id = requestNative(handle);
            return new FutureIpcWireImpl(this, handle, id);
        } catch (IOException e) {
            throw new IOException("the server has declined the connection request");
        }
    }

    public Wire getSessionWire(long handle, long id) throws IOException {
        long sessionId = waitNative(handle, id);
        closeConnectorNative(handle);
        return new WireImpl(new IpcLink(name + "-" + String.valueOf(sessionId)), sessionId);
    }

    public Wire getSessionWire(long handle, long id, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        var timeoutNano = unit.toNanos(timeout);
        if (timeoutNano == Long.MIN_VALUE) {
            throw new IOException("timeout duration overflow");
        }
        long sessionId = waitNative(handle, id, timeoutNano);
        closeConnectorNative(handle);
        return new WireImpl(new IpcLink(name + "-" + String.valueOf(sessionId)), sessionId);
    }

    boolean checkConnection(long handle, long id) {
        return checkNative(handle, id);
    }
}
