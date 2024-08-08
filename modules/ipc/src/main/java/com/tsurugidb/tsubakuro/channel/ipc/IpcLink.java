package com.tsurugidb.tsubakuro.channel.ipc;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.LinkMessage;
import com.tsurugidb.tsubakuro.channel.ipc.sql.ResultSetWireImpl;

/**
 * IpcLink type.
 */
public final class IpcLink extends Link {
    private final long wireHandle;  // for c++
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean serverDown = new AtomicBoolean();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final HashSet<ResultSetWireImpl> resources = new HashSet<>();

    public static final byte RESPONSE_NULL = 0;
    public static final byte RESPONSE_PAYLOAD = 1;
    public static final byte RESPONSE_BODYHEAD = 2;
    public static final byte RESPONSE_CODE = 3;

    private static native long openNative(String name) throws IOException;
    private static native void sendNative(long wireHandle, int slot, byte[] message);
    private static native int awaitNative(long wireHandle, long timeout) throws IOException, TimeoutException;
    private static native int getInfoNative(long wireHandle);
    private static native byte[] receiveNative(long wireHandle);
    private static native boolean isAliveNative(long wireHandle);
    private static native boolean isShutdownNative(long wireHandle);
    private static native void closeNative(long wireHandle);
    private static native void destroyNative(long wireHandle);

    static final Logger LOG = LoggerFactory.getLogger(IpcLink.class);

    static {
        NativeLibrary.load();
    }

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param name the name of shared memory for this IpcLink through which the SQL server is connected
     * @param sessionId the id of this session obtained by the connector requesting a connection to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public IpcLink(@Nonnull String name, long sessionId) throws IOException {
        super.sessionId = sessionId;
        this.wireHandle = openNative(name + "-" + String.valueOf(sessionId));
        LOG.trace("begin Session via shared memory, name = {}", name);
    }

    @Override
    public void send(int s, @Nonnull byte[] frameHeader, @Nonnull byte[] payload, @Nonnull ChannelResponse channelResponse) {
        if (serverDown.get()) {
            channelResponse.setMainResponse(new IOException("Link already closed"));
            return;
        }
        byte[] message = new byte[frameHeader.length + payload.length];
        System.arraycopy(frameHeader, 0, message, 0, frameHeader.length);
        System.arraycopy(payload, 0, message, frameHeader.length, payload.length);

        rwl.readLock().lock();
        try {
            if (!closed.get()) {
                synchronized (this) {
                    sendNative(wireHandle, s, message);
                }
            } else {
                channelResponse.setMainResponse(new IOException("Link already closed"));
                return;
            }
        } finally {
            rwl.readLock().unlock();
        }
        LOG.trace("send {}", payload);
    }

    @Override
    public boolean doPull(long timeout, TimeUnit unit) throws TimeoutException, IOException {
        LinkMessage message = null;
        boolean intentionalClose = true;
        try {
            message = receive(timeout == 0 ? 0 : unit.toMicros(timeout));
        } catch (IOException e) {
            intentionalClose = false;
            throw e;
        }

        if (message != null) {
            if (message.getInfo() != RESPONSE_NULL) {
                if (message.getInfo() == RESPONSE_BODYHEAD) {
                    try {
                        responseBox.pushHead(message.getSlot(), message.getBytes(), createResultSetWire());
                    } catch (IOException e) {
                        responseBox.push(message.getSlot(), e);
                    }
                } else {
                    responseBox.push(message.getSlot(), message.getBytes());
                }
                return true;
            }
            return false;
        }

        // link is closed
        if (!intentionalClose) {
            serverDown.set(true);
        }
        responseBox.doClose(intentionalClose);
        return false;
    }

    private LinkMessage receive(long timeout) throws IOException, TimeoutException {
        rwl.readLock().lock();
        try {
            if (closed.get()) {
                throw new IOException("Link already closed");
            }
            int slot = awaitNative(wireHandle, timeout);
            if (slot >= 0) {
                var info = (byte) getInfoNative(wireHandle);
                return new LinkMessage(info, receiveNative(wireHandle), slot);
            }
            return null;
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        rwl.readLock().lock();
        try {
            if (closed.get()) {
                throw new IOException("Link already closed");
            }
            var rv = new ResultSetWireImpl(wireHandle, this);
            synchronized (resources) {
                resources.add(rv);
            }
            return rv;
        } finally {
            rwl.readLock().unlock();
        }
    }

    public void remove(ResultSetWireImpl resultSetWire) {
        synchronized (resources) {
            resources.remove(resultSetWire);
        }
    }

    @Override
    public boolean isAlive() {
        rwl.readLock().lock();
        try {
            if (closed.get() || (wireHandle == 0)) {
                return false;
            }
            return isAliveNative(wireHandle);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public String linkLostMessage() {
        rwl.readLock().lock();
        try {
            if (closed.get() || (wireHandle == 0)) {
                return "IPC link already closed";
            }
            return isShutdownNative(wireHandle) ? "Session already shutdown" : "IPC connection failure";
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        rwl.writeLock().lock();
        try {
            if (!closed.getAndSet(true)) {
                synchronized (resources) {
                    for (var e : resources) {
                        e.close();
                    }
                }
                closeNative(wireHandle);
                destroyNative(wireHandle);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }
}
