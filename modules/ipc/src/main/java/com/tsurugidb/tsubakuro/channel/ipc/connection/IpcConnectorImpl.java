package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
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
    private long handle;
    private int useCount;

    static {
        NativeLibrary.load();
    }

    public IpcConnectorImpl(String name) {
        this.name = name;
    }

    @Override
    public synchronized FutureResponse<Wire> connect(@Nonnull ClientInformation clientInformation) throws IOException {
        LOG.trace("will connect to {}", name); //$NON-NLS-1$
        if (handle == 0) {
            handle = getConnectorNative(name);
        }
        useCount++;
        try {
            long id = requestNative(handle);
            return new FutureIpcWireImpl(this, id, clientInformation);
        } catch (IOException e) {
            return new FutureIpcWireImpl();  // a future that throws ConnectException on get()
        }
    }

    synchronized WireImpl getSessionWire(long id) throws IOException {
        long sessionId = waitNative(handle, id);
        close();
        return new WireImpl(new IpcLink(name, sessionId));
    }

    synchronized WireImpl getSessionWire(long id, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        var timeoutNano = unit.toNanos(timeout);
        if (timeoutNano == Long.MIN_VALUE) {
            throw new IOException("timeout duration overflow");
        }
        long sessionId = waitNative(handle, id, timeoutNano);
        close();
        return new WireImpl(new IpcLink(name, sessionId));
    }

    synchronized boolean checkConnection(long id) {
        if (handle == 0) {
            return true;
        }
        return checkNative(handle, id);
    }

    private void close() {
        useCount--;
        if (useCount == 0) {
            closeConnectorNative(handle);
            handle = 0;
        }
    }
}