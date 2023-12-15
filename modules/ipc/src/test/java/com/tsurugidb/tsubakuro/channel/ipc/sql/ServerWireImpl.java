package com.tsurugidb.tsubakuro.channel.ipc.sql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.Closeable;
import java.io.IOException;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;

/**
 * ServerWireImpl type.
 */
public class ServerWireImpl implements Closeable {
    private final long wireHandle;  // for c++
    private final String dbName;
    private final Semaphore available = new Semaphore(0, true);
    private long sessionID;
    private final boolean takeSendAction;
    private final ReceiveWorker receiver;
    private SqlRequest.Request sqlRequest;
    private final AtomicBoolean closed = new AtomicBoolean();

    private static native long createNative(String name);
    private static native byte[] getNative(long handle);
    private static native void putNative(long handle, byte[] buffer);
    private static native void closeNative(long handle);
    private static native long createRSLNative(long handle, String name);
    private static native void putRecordsRSLNative(long handle, byte[] buffer);
    private static native void eorRSLNative(long handle);
    private static native void closeRSLNative(long handle);

    static {
        System.loadLibrary("wire-test");
    }

    interface WriteAction {
        void perform(OutputStream buffer) throws IOException, InterruptedException;
    }

    private static byte[] dump(WriteAction action) throws IOException, InterruptedException {
        try (var buffer = new ByteArrayOutputStream()) {
            action.perform(buffer);
            return buffer.toByteArray();
        }
    }

    private class ReceiveWorker extends Thread {
        ReceiveWorker() throws IOException {
        }
        @Override
        public void run() {
            try {
                handshake();
                while (true) {
                    try {
                        var byteArrayInputStream = new ByteArrayInputStream(getNative(wireHandle));
                        if (byteArrayInputStream.available() == 0) {
                            close();
                            return;
                        }
                        var header = FrameworkRequest.Header.parseDelimitedFrom(byteArrayInputStream);
                        sessionID = header.getSessionId();
                        sqlRequest = SqlRequest.Request.parseDelimitedFrom(byteArrayInputStream);
                        available.release();
                    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                        System.err.println(e);
                        e.printStackTrace();
                        throw new IOException("error: ServerWireImpl.get()");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(e);
            }
        }
    }

    public ServerWireImpl(String dbName, long sessionID, boolean takeSendAction) throws IOException {
        this.dbName = dbName;
        this.sessionID = sessionID;
        this.takeSendAction = takeSendAction;
        this.wireHandle = createNative(dbName + "-" + String.valueOf(sessionID));
        if (wireHandle == 0) {
            throw new IOException("error: ServerWireImpl.ServerWireImpl()");
        }
        this.receiver = new ReceiveWorker();
        receiver.start();
    }
    
    public ServerWireImpl(String dbName, long sessionID) throws IOException {
        this(dbName, sessionID, true);
    }

    public void close() throws IOException {
        if (!closed.getAndSet(true)) {
            closeNative(wireHandle);
        }
    }

    public long getSessionID() {
        return sessionID;
    }

    /**
     * implement handshake protocol
     *  step 1) receives the FrameworkRequest.Header and EndpointRequest.Request
     *  step 2) sends a FrameworkResponse.Header and EndpointResponse.Handshake with success
    */
    public void handshake() throws IOException {
        try {
            var byteArrayInputStream = new ByteArrayInputStream(getNative(wireHandle));
            var header = FrameworkRequest.Header.parseDelimitedFrom(byteArrayInputStream);
            var request = EndpointRequest.Request.parseDelimitedFrom(byteArrayInputStream);
            try {
                var response = EndpointResponse.Handshake.newBuilder()
                    .setSuccess(EndpointResponse.Handshake.Success.newBuilder().setSessionId(1))  // who assign this sessionId?
                    .build();
                byte[] resposeByteArray = dump(out -> {
                        FrameworkResponse.Header.newBuilder().build().writeDelimitedTo(out);
                        response.writeDelimitedTo(out);
                    });
                if (wireHandle != 0) {
                    putNative(wireHandle, resposeByteArray);
                } else {
                    throw new IOException("error: sessionWireHandle is 0");
                }
            } catch (IOException | InterruptedException e) {
                throw new IOException(e);
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            System.err.println(e);
            e.printStackTrace();
            throw new IOException("error: ServerWireImpl.getHandshakeRequest()");
        }
    }

    /**
     * Get SqlRequest.Request from a client via the native wire.
     @returns SqlRequest.Request
    */
    public SqlRequest.Request get() throws IOException {
        try {
            available.acquire();
            return sqlRequest;
        } catch(InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * Put SqlResponse.Response to the client via the native wire.
     @param request the SqlResponse.Response message
    */
    public void put(SqlResponse.Response response) throws IOException {
        if (!takeSendAction) {
            return;
        }
        try {
            byte[] resposeByteArray = dump(out -> {
                    FrameworkResponse.Header.newBuilder().build().writeDelimitedTo(out);
                    response.writeDelimitedTo(out);
                });
            if (wireHandle != 0) {
                putNative(wireHandle, resposeByteArray);
            } else {
                throw new IOException("error: sessionWireHandle is 0");
            }
        } catch (IOException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    public long createRSL(String name) throws IOException {
        if (wireHandle != 0) {
            return createRSLNative(wireHandle, name);
        } else {
            throw new IOException("error: ServerWireImpl.createRSL()");
        }
    }

    public void putRecordsRSL(long handle, byte[] ba) throws IOException {
        if (!takeSendAction) {
            return;
        }
        if (handle != 0) {
            putRecordsRSLNative(handle, ba);
        } else {
            throw new IOException("error: resultSetWireHandle is 0");
        }
    }

    public void eorRSL(long handle) throws IOException {
        if (!takeSendAction) {
            return;
        }
        if (handle != 0) {
            eorRSLNative(handle);
        } else {
            throw new IOException("error: resultSetWireHandle is 0");
        }
    }
}
