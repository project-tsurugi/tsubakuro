package com.nautilus_technologies.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Credential;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.channel.ipc.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * IpcConnectorImpl type.
 */
public final class IpcConnectorImpl implements Connector {

    private static final Logger LOG = LoggerFactory.getLogger(IpcConnectorImpl.class);

    private static native long getConnectorNative(String name) throws IOException;
    private static native long requestNative(long handle);
    private static native void waitNative(long handle, long id);
    private static native void waitNative(long handle, long id, long timeout) throws TimeoutException;
    private static native boolean checkNative(long handle, long id);
    private static native void closeConnectorNative(long handle);

    static {
        System.loadLibrary("wire");
    }

    private final String name;
    long handle;
    long id;

    public IpcConnectorImpl(String name) {
        this.name = name;
    }

    @Override
    public FutureResponse<Wire> connect(Credential credential) throws IOException {
        LOG.trace("will connect to {}", name); //$NON-NLS-1$

        handle = getConnectorNative(name);
        id = requestNative(handle);
        return new FutureSessionWireImpl(this);
    }

    public Wire getSessionWire() throws IOException {
        waitNative(handle, id);
        close();
        return new SessionWireImpl(name, id);
    }

    public Wire getSessionWire(long timeout, TimeUnit unit) throws TimeoutException, IOException {
        var timeoutNano = unit.toNanos(timeout);
        if (timeoutNano == Long.MIN_VALUE) {
            throw new IOException("timeout duration overflow");
        }
        waitNative(handle, id, timeoutNano);
        close();
        return new SessionWireImpl(name, id);
    }

    public boolean checkConnection() {
        return checkNative(handle, id);
    }

    /**
     * Close the wire
     * @throws IOException close error
     */
    public void close() throws IOException {
        if (handle != 0) {
            closeConnectorNative(handle);
        }
        handle = 0;
    }
}
