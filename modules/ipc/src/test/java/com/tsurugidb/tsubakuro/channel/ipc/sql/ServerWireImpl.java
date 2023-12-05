package com.tsurugidb.tsubakuro.channel.ipc.sql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.Closeable;
import java.io.IOException;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;

/**
 * ServerWireImpl type.
 */
public class ServerWireImpl implements Closeable {
    private long wireHandle = 0;  // for c++
    private String dbName;
    private long sessionID;

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

    public ServerWireImpl(String dbName, long sessionID) throws IOException {
        this.dbName = dbName;
        this.sessionID = sessionID;
        wireHandle = createNative(dbName + "-" + String.valueOf(sessionID));
        if (wireHandle == 0) {
            throw new IOException("error: ServerWireImpl.ServerWireImpl()");
        }
    }

    public void close() throws IOException {
    if (wireHandle != 0) {
        closeNative(wireHandle);
        wireHandle = 0;
    }
    }

    public long getSessionID() {
        return sessionID;
    }

    /**
     * Get SqlRequest.Request from a client via the native wire.
     @returns SqlRequest.Request
    */
    public SqlRequest.Request get() throws IOException {
        try {
            var byteArrayInputStream = new ByteArrayInputStream(getNative(wireHandle));
            var header = FrameworkRequest.Header.parseDelimitedFrom(byteArrayInputStream);
            sessionID = header.getSessionId();
            return SqlRequest.Request.parseDelimitedFrom(byteArrayInputStream);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            System.err.println(e);
            e.printStackTrace();
            throw new IOException("error: ServerWireImpl.get()");
        }
    }

    /**
     * Put SqlResponse.Response to the client via the native wire.
     @param request the SqlResponse.Response message
    */
    public void put(SqlResponse.Response response) throws IOException {
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
        if (handle != 0) {
            putRecordsRSLNative(handle, ba);
        } else {
            throw new IOException("error: resultSetWireHandle is 0");
        }
    }

    public void eorRSL(long handle) throws IOException {
        if (handle != 0) {
            eorRSLNative(handle);
        } else {
            throw new IOException("error: resultSetWireHandle is 0");
        }
    }
}
