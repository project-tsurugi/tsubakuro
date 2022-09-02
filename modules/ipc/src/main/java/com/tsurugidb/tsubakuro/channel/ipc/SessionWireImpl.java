package com.tsurugidb.tsubakuro.channel.ipc;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseWireHandle;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.ipc.sql.ResultSetWireImpl;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.Owner;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements Wire {
    static final FrameworkRequest.Header.Builder HEADER_BUILDER = FrameworkRequest.Header.newBuilder().setMessageVersion(1);

    private long wireHandle = 0;  // for c++
    private final String dbName;
    private final long sessionID;
    private final Queue<QueueEntry> queue;

    private static native long openNative(String name) throws IOException;
    private static native long getResponseHandleNative(long wireHandle);
    private static native void sendNative(long sessionHandle, byte[] buffer);
    private static native void sendNative(long sessionHandle, ByteBuffer buffer);
    private static native void setQueryModeNative(long responseWireHandle);
    private static native void flushNative(long wireHandle);
    private static native ByteBuffer receiveNative(long responseWireHandle);
    private static native ByteBuffer receiveNative(long responseWireHandle, long timeout) throws TimeoutException;
    private static native void releaseNative(long responseWireHandle);
    private static native void closeNative(long wireHandle);

    static final Logger LOG = LoggerFactory.getLogger(SessionWireImpl.class);

    static {
        System.loadLibrary("wire");
    }

    static class QueueEntry {
        final long serviceId;
        final byte[] request;
        final ChannelResponse response;

        QueueEntry(long serviceId, byte[] request, ChannelResponse response) {
            this.serviceId = serviceId;
            this.request = request;
            this.response = response;
        }
        long serviceId() {
            return serviceId;
        }
        byte[] getRequest() {
            return request;
        }
        ChannelResponse getResponse() {
            return response;
        }
    }

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param dbName the name of the SQL server to which this SessionWireImpl is to be connected
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public SessionWireImpl(String dbName, long sessionID) throws IOException {
        wireHandle = openNative(dbName + "-" + String.valueOf(sessionID));
        this.dbName = dbName;
        this.sessionID = sessionID;
        this.queue = new ArrayDeque<>();
        LOG.trace("begin Session via stream, id = {}", sessionID);
    }

    /**
     * Send a Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in byte[]
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in sendNative()
     */
    @Override
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var response = new ChannelResponse(this);
        var future = FutureResponse.wrap(Owner.of(response));
        synchronized (this) {
            var header = HEADER_BUILDER.setServiceId(serviceId).setSessionId(sessionID).build();
            var handle = getResponseHandleNative(wireHandle);
            if (handle != 0) {
                response.setResponseHandle(new ResponseWireHandleImpl(handle));
                sendNative(wireHandle, toDelimitedByteArray(header));
                sendNative(wireHandle, payload);
                flushNative(wireHandle);
                LOG.trace("send {}, handle = {}", payload, handle);
            } else {
                queue.add(new QueueEntry(serviceId, payload, response));
            }
        }
        return future;
    }

    /**
     * Send a Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in ByteBuffer
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in sendNative()
     */
    @Override
    public  FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var response = new ChannelResponse(this);
        var future = FutureResponse.wrap(Owner.of(response));
        synchronized (this) {
            var header = HEADER_BUILDER.setServiceId(serviceId).setSessionId(sessionID).build();
            var handle = getResponseHandleNative(wireHandle);
            if (handle != 0) {
                response.setResponseHandle(new ResponseWireHandleImpl(handle));
                sendNative(wireHandle, toDelimitedByteArray(header));
                if (payload.isDirect()) {
                    sendNative(wireHandle, payload);
                } else {
                    sendNative(wireHandle, payload.array());
                }
                flushNative(wireHandle);
                LOG.trace("send {}, handle = {}", payload, handle);
            } else {
                queue.add(new QueueEntry(serviceId, payload.array(), response));  // FIXME in case of Direct ByteBuffer
            }
        }
        return future;
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var responseWireHandle = ((ResponseWireHandleImpl) handle).getHandle();
        var byteBuffer = receiveNative(responseWireHandle);
        FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(byteBuffer));
        return byteBuffer;
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var responseWireHandle = ((ResponseWireHandleImpl) handle).getHandle();
        var timeoutNano = unit.toNanos(timeout);
        if (timeoutNano == Long.MIN_VALUE) {
            throw new IOException("timeout duration overflow");
        }
        var byteBuffer = receiveNative(responseWireHandle, timeoutNano);
        FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(byteBuffer));
        return byteBuffer;
    }

    /**
     * Set to receive a Query type response by response box
     */
    @Override
    public void setResultSetMode(ResponseWireHandle handle) {
        setQueryModeNative(((ResponseWireHandleImpl) handle).getHandle());
    }

    /**
     * release the message in the response box
     * @param handle the handle to the response box
    */
    @Override
    public void release(ResponseWireHandle handle) throws IOException {
        releaseNative(((ResponseWireHandleImpl) handle).getHandle());

        synchronized (this) {
            var entry = queue.peek();
            if (Objects.nonNull(entry)) {
                var nextHandle = getResponseHandleNative(wireHandle);
                if (nextHandle != 0) {
                    entry.getResponse().setResponseHandle(new ResponseWireHandleImpl(nextHandle));

                    var header = HEADER_BUILDER.setServiceId(entry.serviceId()).setSessionId(sessionID).build();
                    sendNative(wireHandle, toDelimitedByteArray(header));
                    sendNative(wireHandle, entry.getRequest());
                    flushNative(wireHandle);
                    queue.poll();
                    LOG.trace("send {}, handle = {}", entry.getRequest(), handle);
                }
            }
        }
    }

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return ResultSetWireImpl
    */
    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        return new ResultSetWireImpl(wireHandle);
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        closeNative(wireHandle);
        wireHandle = 0;
    }

    byte[] toDelimitedByteArray(FrameworkRequest.Header request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public String getDbName() {
        return dbName;
    }
}
