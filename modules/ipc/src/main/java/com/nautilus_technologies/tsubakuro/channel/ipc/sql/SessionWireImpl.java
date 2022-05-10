package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.sql.FutureQueryResponseImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.FutureResponseImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.nautilus_technologies.tateyama.proto.FrameworkRequestProtos;
import com.nautilus_technologies.tateyama.proto.FrameworkResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements SessionWire {
    static final FrameworkRequestProtos.Header.Builder HEADER_BUILDER = FrameworkRequestProtos.Header.newBuilder().setMessageVersion(1);

    private long wireHandle = 0;  // for c++
    private final String dbName;
    private final long sessionID;
    private final NativeOutputStream nativeOutputStream;
    private final Queue<QueueEntry<?>> queue;

    private static native long openNative(String name) throws IOException;
    private static native long getResponseHandleNative(long wireHandle);
    private static native void sendNative(long wireHandle, int b);
    private static native void flushNative(long wireHandle, long responseHandle, boolean isQuery);
    private static native ByteBuffer receiveNative(long responseHandle);
    private static native ByteBuffer receiveNative(long responseHandle, long timeout) throws TimeoutException;
    private static native void unReceiveNative(long responseHandle);
    private static native void releaseNative(long responseHandle);
    private static native void closeNative(long wireHandle);

    final Logger logger = LoggerFactory.getLogger(SessionWireImpl.class);

    static {
        System.loadLibrary("wire");
    }

    static class NativeOutputStream extends OutputStream {
        private long wireHandle;

        NativeOutputStream(long wireHandle) {
            this.wireHandle = wireHandle;
        }
        public void write(int b) {
            sendNative(wireHandle, b);
        }
        public long getResponseHandle() {
            return getResponseHandleNative(wireHandle);
        }
        public void flush(long responseHandle, boolean isQuery) {
            flushNative(wireHandle, responseHandle, isQuery);
        }
    }

    static class ByteBufferBackedInputStream extends InputStream {
        ByteBuffer buf;
        ByteBufferBackedInputStream(ByteBuffer buf) {
            this.buf = buf;
        }
        public int read() throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get() & 0xFF;
        }
        public int read(byte[] bytes, int off, int len)
            throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }
    }

    enum RequestType {
        STATEMENT,
        QUERY
    };

    static class QueueEntry<V> {
        RequestType type;
        RequestProtos.Request request;
        FutureResponseImpl<V> futureBody;
        FutureQueryResponseImpl futureHead;

        QueueEntry(RequestProtos.Request request, FutureQueryResponseImpl futureHead, FutureResponseImpl<V> futureBody) {
            this.type = RequestType.QUERY;
            this.request = request;
            this.futureBody = futureBody;
            this.futureHead = futureHead;
        }
        QueueEntry(RequestProtos.Request request, FutureResponseImpl<V> futureBody) {
            this.type = RequestType.STATEMENT;
            this.request = request;
            this.futureBody = futureBody;
        }
        RequestType getRequestType() {
            return type;
        }
        RequestProtos.Request getRequest() {
            return request;
        }
        FutureQueryResponseImpl getFutureHead() {
            return futureHead;
        }
        FutureResponseImpl<V> getFutureBody() {
            return futureBody;
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
        this.nativeOutputStream = new NativeOutputStream(wireHandle);
        this.queue = new ArrayDeque<>();
        logger.trace("begin Session via stream, id = " + sessionID);
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        closeNative(wireHandle);
        wireHandle = 0;
    }

    /**
     * Send RequestProtos.Request to the SQL server via the native wire.
     * @param request the RequestProtos.Request message
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in sendNative()
     */
    @Override
    public <V> FutureResponse<V> send(long serviceID, RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var header = HEADER_BUILDER.setServiceId(serviceID).setSessionId(sessionID).build();
        var req = request.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(sessionID)).build();
        var futureBody = new FutureResponseImpl<V>(this, distiller);
        synchronized (this) {
            var handle = nativeOutputStream.getResponseHandle();
            if (handle != 0) {
                header.writeDelimitedTo(nativeOutputStream);
                req.writeDelimitedTo(nativeOutputStream);
                nativeOutputStream.flush(handle, false);
                futureBody.setResponseHandle(new ResponseWireHandleImpl(handle));
                logger.trace("send " + request + ", handle = " + handle);
            } else {
                queue.add(new QueueEntry<V>(req, futureBody));
            }
        }
        return futureBody;
    }

    /**
     * Send RequestProtos.Request to the SQL server via the native wire.
     * @param request the RequestProtos.Request message
     * @return a couple of Future response message corresponding the request
     */
    @Override
    public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> sendQuery(
            long serviceID, RequestProtos.Request.Builder request) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        var header = HEADER_BUILDER.setServiceId(serviceID).setSessionId(sessionID).build();
        var req = request.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(sessionID)).build();
        var left = new FutureQueryResponseImpl(this);
        var right = new FutureResponseImpl<ResponseProtos.ResultOnly>(this, new ResultOnlyDistiller());
        synchronized (this) {
            var handle = nativeOutputStream.getResponseHandle();
            if (handle != 0) {
                header.writeDelimitedTo(nativeOutputStream);
                req.writeDelimitedTo(nativeOutputStream);
                nativeOutputStream.flush(handle, true);
                left.setResponseHandle(new ResponseWireHandleImpl(handle));
                right.setResponseHandle(new ResponseWireHandleImpl(handle));
                logger.trace("send " + request + ", handle = " + handle);
            } else {
                queue.add(new QueueEntry<ResponseProtos.ResultOnly>(req, left, right));
            }
        }
        return Pair.of(left, right);
    }

    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     * @param handle the handle indicating the sent request message corresponding to the response message to be received.
     * @return ResposeProtos.Response message
    */
    @Override
    public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        try {
            var responseHandle = ((ResponseWireHandleImpl) handle).getHandle();
            var byteBufferInput = new ByteBufferBackedInputStream(receiveNative(responseHandle));
            FrameworkResponseProtos.Header.parseDelimitedFrom(byteBufferInput);
            var response = ResponseProtos.Response.parseDelimitedFrom(byteBufferInput);
            logger.trace("receive " + response + ", hancle = " + handle);
            synchronized (this) {
                releaseNative(responseHandle);
                var entry = queue.peek();
                if (!Objects.isNull(entry)) {
                    long responseBoxHandle = nativeOutputStream.getResponseHandle();
                    if (responseBoxHandle != 0) {
                        if (entry.getRequestType() == RequestType.STATEMENT) {
                            entry.getRequest().writeTo(nativeOutputStream);
                            nativeOutputStream.flush(responseBoxHandle, false);
                            entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
                            queue.poll();
                        } else {
                            entry.getRequest().writeTo(nativeOutputStream);
                            nativeOutputStream.flush(responseBoxHandle, true);
                            entry.getFutureHead().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
                            entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
                            queue.poll();
                        }
                    }
                }
            }
            return response;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: SessionWireImpl.receive()", e);
        }
    }

    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     * @param handle the handle indicating the sent request message corresponding to the response message to be received.
     * @return response message of ResposeProtos.Response type
    */
    @Override
    public ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit)
            throws TimeoutException, IOException {
        if (wireHandle == 0) {
            throw new IOException("already closed");
        }
        try {
            var responseHandle = ((ResponseWireHandleImpl) handle).getHandle();
            var timeoutNano = unit.toNanos(timeout);
            if (timeoutNano == Long.MIN_VALUE) {
                throw new IOException("timeout duration overflow");
            }
            var byteBufferInput = new ByteBufferBackedInputStream(receiveNative(responseHandle, timeoutNano));
            FrameworkResponseProtos.Header.parseDelimitedFrom(byteBufferInput);
            var response = ResponseProtos.Response.parseDelimitedFrom(byteBufferInput);
            synchronized (this) {
                releaseNative(responseHandle);
                var entry = queue.peek();
                if (!Objects.isNull(entry)) {
                    long responseBoxHandle = nativeOutputStream.getResponseHandle();
                    if (responseBoxHandle != 0) {
                        if (entry.getRequestType() == RequestType.STATEMENT) {
                            entry.getRequest().writeTo(nativeOutputStream);
                            nativeOutputStream.flush(responseBoxHandle, false);
                            entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
                            queue.poll();
                        } else {
                            entry.getRequest().writeTo(nativeOutputStream);
                            nativeOutputStream.flush(responseBoxHandle, true);
                            entry.getFutureHead().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
                            entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(responseBoxHandle));
                            queue.poll();
                        }
                    }
                }
            }
            return response;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: SessionWireImpl.receive()", e);
        }
    }

    /**
     * UnReceive one ResponseProtos.Response
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

    public String getDbName() {
        return dbName;
    }
}
