package com.tsurugidb.tsubakuro.channel.ipc;

import java.io.IOException;
import java.util.Objects;
// import java.util.Objects;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.TimeoutException;

// import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.ipc.sql.ResultSetWireImpl;

/**
 * IpcWire type.
 */
public class IpcWire extends Thread {

    static final Logger LOG = LoggerFactory.getLogger(IpcWire.class);

    private ResponseBox responseBox = new ResponseBox(this);
    private long wireHandle = 0;  // for c++
    private boolean closed = false;

    public static final byte RESPONSE_NULL = 0;
    public static final byte RESPONSE_PAYLOAD = 1;
    public static final byte RESPONSE_BODYHEAD = 2;
    public static final byte RESPONSE_CODE = 3;

    private static native long openNative(String name) throws IOException;
    private static native void sendNative(long wireHandle, int slot, byte[] header, byte[] payload);
    private static native int awaitNative(long wireHandle) throws IOException;
//    private static native int awaitNative(long wireHandle, long timeout) throws TimeoutException;
    private static native int getInfoNative(long wireHandle);
    private static native byte[] receiveNative(long wireHandle);
    private static native void closeNative(long wireHandle);

    private static class IpcMessage {
        public final byte[] bytes;
        private final byte info;
        private final int slot;
    
        IpcMessage(byte info, byte[] bytes, int slot) {
            this.info = info;
            this.bytes = bytes;
            this.slot = slot;
        }
        public byte getInfo() {  // used only by FutureSessionWireImpl
            return info;
        }
        public byte[] getBytes() {
            return bytes;
        }
        public int getSlot() {
            return slot;
        }
    }

    static {
        NativeLibrary.load();
    }

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param name the name of shared memory for this IpcWire through which the SQL server is connected
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public IpcWire(String name) throws IOException {
        this.wireHandle = openNative(name);
        LOG.trace("begin Session via shared memory, name = {}", name);
    }

    public ResponseBox getResponseBox() {
        return responseBox;
    }

    public void send(int s, byte[] frameHeader, byte[] payload) {
        sendNative(wireHandle, s, frameHeader, payload);
        LOG.trace("send {}", payload);
    }

    private boolean pull() {
        var message = receive();

        if (Objects.isNull(message)) {
            return false;
        }
        if (message.getInfo() != RESPONSE_NULL) {
            responseBox.push(message.getSlot(), message.getBytes(), message.getInfo() == RESPONSE_BODYHEAD);
            return true;
        }
        return false;
    }

    public IpcMessage receive() {
        int slot;
        try {
            slot = awaitNative(wireHandle);
        } catch (IOException e) {
            return null;
        }
        var info = (byte) getInfoNative(wireHandle);
        return new IpcMessage(info, receiveNative(wireHandle), slot);
    }

    public ResultSetWire createResultSetWire() throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        return new ResultSetWireImpl(wireHandle);
    }

    public int lookFor() {
        synchronized (this) {
//            return getResponseHandleNative(wireHandle);
return 0;
        }
    }

    public void run() {
        while (true) {
            if (!pull()) {
                break;
            }
        }
    }

    public void close() throws IOException {
        if (!closed) {
            closeNative(wireHandle);
            closed = true;
        }
        try {
            this.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
