package com.tsurugidb.tsubakuro.channel.ipc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.LinkMessage;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.ipc.sql.ResultSetWireImpl;

/**
 * IpcLink type.
 */
public final class IpcLink extends Link {
    private long wireHandle = 0;  // for c++
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean serverDown = new AtomicBoolean();
    private Receiver receiver;

    public static final byte RESPONSE_NULL = 0;
    public static final byte RESPONSE_PAYLOAD = 1;
    public static final byte RESPONSE_BODYHEAD = 2;
    public static final byte RESPONSE_CODE = 3;

    private static native long openNative(String name) throws IOException;
    private static native void sendNative(long wireHandle, int slot, byte[] header, byte[] payload);
    private static native int awaitNative(long wireHandle) throws IOException;
    private static native int getInfoNative(long wireHandle);
    private static native byte[] receiveNative(long wireHandle);
    private static native boolean isAliveNative(long wireHandle);
    private static native void closeNative(long wireHandle);
    private static native void destroyNative(long wireHandle);

    static final Logger LOG = LoggerFactory.getLogger(IpcLink.class);

    static {
        NativeLibrary.load();
    }

    private class Receiver extends Thread {
        public void run() {
            while (true) {
                if (!pull()) {
                    break;
                }
            }
        }
    }

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param name the name of shared memory for this IpcLink through which the SQL server is connected
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public IpcLink(@Nonnull String name) throws IOException {
        this.wireHandle = openNative(name);
        this.receiver = new Receiver();
        receiver.start();
        LOG.trace("begin Session via shared memory, name = {}", name);
    }

    @Override
    public void send(int s, @Nonnull byte[] frameHeader, @Nonnull byte[] payload, @Nonnull ChannelResponse channelResponse) {
        if (serverDown.get()) {
            channelResponse.setMainResponse(new IOException("Link already closed"));
            return;
        }
        synchronized (this) {
            sendNative(wireHandle, s, frameHeader, payload);
        }
        LOG.trace("send {}", payload);
    }

    private boolean pull() {
        try {
            var message = receive();

            if (Objects.isNull(message)) {
                responseBox.close();
                return false;
            }
            if (message.getInfo() != RESPONSE_NULL) {
                if (message.getInfo() == RESPONSE_BODYHEAD) {
                    responseBox.pushHead(message.getSlot(), message.getBytes(), createResultSetWire());
                } else {
                    responseBox.push(message.getSlot(), message.getBytes());
                }
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private LinkMessage receive() {
        try {
            int slot = awaitNative(wireHandle);
            if (slot >= 0) {
                var info = (byte) getInfoNative(wireHandle);
                return new LinkMessage(info, receiveNative(wireHandle), slot);
            }
            return null;
        } catch (IOException e) {
            serverDown.set(true);
            return null;
        }
    }

    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        return new ResultSetWireImpl(wireHandle);
    }

    @Override
    public boolean isAlive() {
        if (closed.get() || (wireHandle == 0)) {
            return false;
        }
        return isAliveNative(wireHandle);
    }

    @Override
    public void close() throws IOException {
        if (!closed.get()) {
            closeNative(wireHandle);
            try {
                receiver.join();
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                destroyNative(wireHandle);
                closed.set(true);
            }
        }
    }
}
