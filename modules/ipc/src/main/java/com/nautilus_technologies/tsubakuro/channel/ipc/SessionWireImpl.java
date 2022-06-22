package com.nautilus_technologies.tsubakuro.channel.ipc;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ChannelResponse;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.channel.ipc.sql.ResultSetWireImpl;
import com.nautilus_technologies.tateyama.proto.FrameworkRequestProtos;
import com.nautilus_technologies.tateyama.proto.FrameworkResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;
import com.nautilus_technologies.tsubakuro.util.Owner;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements SessionWire {
    static final FrameworkRequestProtos.Header.Builder HEADER_BUILDER = FrameworkRequestProtos.Header.newBuilder().setMessageVersion(1);

    private long wireHandle = 0;  // for c++
    private final String dbName;
    private final long sessionID;
    private final Queue<QueueEntry> queue;

    private static native long openNative(String name) throws IOException;
    private static native long getResponseHandleNative(long wireHandle);
    private static native void sendNative(long sessionHandle, byte[] buffer);
    private static native void sendNative(long sessionHandle, ByteBuffer buffer);
    private static native void setQueryModeNative(long responseHandle);
    private static native void flushNative(long wireHandle);
    private static native ByteBuffer receiveNative(long responseHandle);
    private static native ByteBuffer receiveNative(long responseHandle, long timeout) throws TimeoutException;
    private static native void unReceiveNative(long responseHandle);
    private static native void releaseNative(long responseHandle);
    private static native void closeNative(long wireHandle);

    final Logger logger = LoggerFactory.getLogger(SessionWireImpl.class);

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
        logger.trace("begin Session via stream, id = " + sessionID);
    }

    /**
     * Send a Request to the server via the native wire.
     * @param request the Request message in byte[]
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in sendNative()
     */
    @Override
    public FutureResponse<? extends Response> send(long serviceId, byte[] request) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var response = new ChannelResponse(this);
        var future = FutureResponse.wrap(Owner.of(response));
        var header = HEADER_BUILDER.setServiceId(serviceId).setSessionId(sessionID).build();
        synchronized (this) {
            var handle = getResponseHandleNative(wireHandle);
            if (handle != 0) {
                sendNative(wireHandle, toDelimitedByteArray(header));
                sendNative(wireHandle, request);
                flushNative(wireHandle);
                response.setResponseHandle(new ResponseWireHandleImpl(handle));
                logger.trace("send " + request + ", handle = " + handle);  // FIXME use formatted message
            } else {
                queue.add(new QueueEntry(serviceId, request, response));
            }
        }
        return future;
    }

    /**
     * Send a Request to the server via the native wire.
     * @param request the Request message in ByteBuffer
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in sendNative()
     */
    @Override
    public FutureResponse<? extends Response> send(long serviceId, ByteBuffer request) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var response = new ChannelResponse(this);
        var future = FutureResponse.wrap(Owner.of(response));
        var header = HEADER_BUILDER.setServiceId(serviceId).setSessionId(sessionID).build();
        synchronized (this) {
            var handle = getResponseHandleNative(wireHandle);
            if (handle != 0) {
                sendNative(wireHandle, toDelimitedByteArray(header));
                if (request.isDirect()) {
                    sendNative(wireHandle, request);
                } else {
                    sendNative(wireHandle, request.array());
                }
                flushNative(wireHandle);
                response.setResponseHandle(new ResponseWireHandleImpl(handle));
                logger.trace("send " + request + ", handle = " + handle);  // FIXME use formatted message
            } else {
                queue.add(new QueueEntry(serviceId, request.array(), response));  // FIXME in case of Direct ByteBuffer
            }
        }
        return future;
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var responseHandle = ((ResponseWireHandleImpl) handle).getHandle();
        var byteBuffer = receiveNative(responseHandle);
        FrameworkResponseProtos.Header.parseDelimitedFrom(new ByteBufferInputStream(byteBuffer));

        synchronized (this) {
            releaseNative(responseHandle);
            var entry = queue.peek();
            if (Objects.nonNull(entry)) {
                var nextHandle = getResponseHandleNative(wireHandle);
                if (nextHandle != 0) {
                    entry.getResponse().setResponseHandle(new ResponseWireHandleImpl(nextHandle));

                    var header = HEADER_BUILDER.setServiceId(entry.serviceId()).setSessionId(sessionID).build();
                    sendNative(nextHandle, toDelimitedByteArray(header));
                    sendNative(nextHandle, entry.getRequest());
                    flushNative(nextHandle);
                    queue.poll();
                    logger.trace("send " + entry.getRequest() + ", handle = " + handle);  // FIXME use formatted message
                }
            }
        }

        return byteBuffer;
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var responseHandle = ((ResponseWireHandleImpl) handle).getHandle();
        var timeoutNano = unit.toNanos(timeout);
        if (timeoutNano == Long.MIN_VALUE) {
            throw new IOException("timeout duration overflow");
        }
        System.out.println("import static org.junit.jupiter.api.Assertions.assertTrue;" + timeoutNano);
        var byteBuffer = receiveNative(responseHandle, timeoutNano);
        releaseNative(responseHandle);
        FrameworkResponseProtos.Header.parseDelimitedFrom(new ByteBufferInputStream(byteBuffer));

        synchronized (this) {
            releaseNative(responseHandle);
            var entry = queue.peek();
            if (Objects.nonNull(entry)) {
                var nextHandle = getResponseHandleNative(wireHandle);
                if (nextHandle != 0) {
                    entry.getResponse().setResponseHandle(new ResponseWireHandleImpl(nextHandle));

                    var header = HEADER_BUILDER.setServiceId(entry.serviceId()).setSessionId(sessionID).build();
                    sendNative(nextHandle, toDelimitedByteArray(header));
                    sendNative(nextHandle, entry.getRequest());
                    flushNative(nextHandle);
                    queue.poll();
                    logger.trace("send " + entry.getRequest() + ", handle = " + handle);  // FIXME use formatted message
                }
            }
        }

        return byteBuffer;
    }

    /**
     * Set to receive a Query type response by response box
     */
    @Override
    public void setQueryMode(ResponseWireHandle handle) {
        setQueryModeNative(((ResponseWireHandleImpl) handle).getHandle());
    }

    /**
     * UnReceive one SqlResponse.Response
     * @param handle the handle to the response box
    */
    @Override
    public void unReceive(ResponseWireHandle handle) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        unReceiveNative(((ResponseWireHandleImpl) handle).getHandle());
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

    byte[] toDelimitedByteArray(FrameworkRequestProtos.Header request) throws IOException {
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
