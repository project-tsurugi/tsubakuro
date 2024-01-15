package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
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
    private static final Cleaner CLEANER = Cleaner.create();
    private final CloseRunnable closeRunnable;
    private final Cleaner.Cleanable cleanable;
    private long handle;

    static {
        NativeLibrary.load();
    }

    public IpcConnectorImpl(String name) {
        this.name = name;

        // for GC
        this.closeRunnable = new CloseRunnable();
        this.cleanable = CLEANER.register(this, closeRunnable);
    }

    @Override
    public FutureResponse<Wire> connect(@Nonnull Credential credential, @Nonnull ClientInformation clientInformation) throws IOException {
        LOG.trace("will connect to {}", name); //$NON-NLS-1$

        if (handle == 0) {
            handle = getConnectorNative(name);
        }
        try {
            long id = requestNative(handle);
            return new FutureIpcWireImpl(this, id, credential, clientInformation);
        } catch (IOException e) {
            throw new ConnectException("the server has declined the connection request");
        }
    }

    public Wire getSessionWire(long id) throws IOException {
        long sessionId = waitNative(handle, id);
        return new WireImpl(new IpcLink(name + "-" + String.valueOf(sessionId)), sessionId);
    }

    public Wire getSessionWire(long id, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        var timeoutNano = unit.toNanos(timeout);
        if (timeoutNano == Long.MIN_VALUE) {
            throw new IOException("timeout duration overflow");
        }
        long sessionId = waitNative(handle, id, timeoutNano);
        return new WireImpl(new IpcLink(name + "-" + String.valueOf(sessionId)), sessionId);
    }

    boolean checkConnection(long id) {
        return checkNative(handle, id);
    }

    private class CloseRunnable implements Runnable {
        CloseRunnable() {
        }
        public void run() {
            closeConnectorNative(handle);
        }
    }
}
