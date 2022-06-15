package com.nautilus_technologies.tsubakuro.channel.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
// import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ChannelResponse;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.channel.common.sql.FutureQueryResponseImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.FutureResponseImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.stream.sql.ResultSetWireImpl;
import com.nautilus_technologies.tsubakuro.channel.stream.sql.ResponseBox;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tateyama.proto.FrameworkRequestProtos;
import com.nautilus_technologies.tateyama.proto.FrameworkResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ByteBufferInputStream;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.util.Owner;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements SessionWire {
    static final FrameworkRequestProtos.Header.Builder HEADER_BUILDER = FrameworkRequestProtos.Header.newBuilder().setMessageVersion(1);

    private StreamWire streamWire;
    private final long sessionID;
    private final ResponseBox responseBox;
    private final Queue<QueueEntry> queue;

    enum RequestType {
        STATEMENT,
        QUERY
    };

    static class QueueEntry<V> {
        RequestType type;
        byte[] request;
        FutureResponseImpl<V> futureBody;
        FutureQueryResponseImpl futureHead;

        QueueEntry(byte[] request, FutureQueryResponseImpl futureHead, FutureResponseImpl<V> futureBody) {
            this.type = RequestType.QUERY;
            this.request = request;
            this.futureBody = futureBody;
            this.futureHead = futureHead;
        }
        QueueEntry(byte[] request, FutureResponseImpl<V> futureBody) {
            this.type = RequestType.STATEMENT;
            this.request = request;
            this.futureBody = futureBody;
        }
        RequestType getRequestType() {
            return type;
        }
        byte[] getRequest() {
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
     * @param streamWire the stream object by which this SessionWireImpl is connected to the SQL server
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     */
    public SessionWireImpl(StreamWire streamWire, long sessionID) {
        this.streamWire = streamWire;
        this.sessionID = sessionID;
        this.responseBox = streamWire.getResponseBox();
        this.queue = new ArrayDeque<>();
        LoggerFactory.getLogger(SessionWireImpl.class).trace("begin Session via stream, id = " + sessionID);
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        streamWire.close();
        streamWire = null;
    }

    /**
     * Send SqlRequest.Request to the SQL server via the native wire.
     @param request the SqlRequest.Request message
     @return a FutureResponse response message corresponding the request
     */
    @Override
    public <V> FutureResponse<V> send(long serviceID, SqlRequest.Request.Builder request, Distiller<V> distiller) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        var header = HEADER_BUILDER.setServiceId(serviceID).setSessionId(sessionID).build();
        var req = request.setSessionHandle(SqlCommon.Session.newBuilder().setHandle(sessionID)).build();
        try (var buffer = new ByteArrayOutputStream()) {
            header.writeDelimitedTo(buffer);
            req.writeDelimitedTo(buffer);
            var bytes = buffer.toByteArray();
            var futureBody = new FutureResponseImpl<V>(this, distiller);
            var index = responseBox.lookFor(1);
            if (index >= 0) {
                streamWire.send(index, bytes);
                futureBody.setResponseHandle(new ResponseWireHandleImpl(index));
            } else {
                queue.add(new QueueEntry<V>(bytes, futureBody));
            }
            return futureBody;
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    /**
     * Send SqlRequest.Request to the SQL server via the native wire.
     @param request the SqlRequest.Request message
     @return a couple of FutureResponse response message corresponding the request
     */
    @Override
    public Pair<FutureResponse<SqlResponse.ExecuteQuery>, FutureResponse<SqlResponse.ResultOnly>> sendQuery(long serviceID, SqlRequest.Request.Builder request) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        var header = HEADER_BUILDER.setServiceId(serviceID).setSessionId(sessionID).build();
        var req = request.setSessionHandle(SqlCommon.Session.newBuilder().setHandle(sessionID)).build();
        try (var buffer = new ByteArrayOutputStream()) {
            header.writeDelimitedTo(buffer);
            req.writeDelimitedTo(buffer);
            var bytes = buffer.toByteArray();

            var left = new FutureQueryResponseImpl(this);
            var right = new FutureResponseImpl<SqlResponse.ResultOnly>(this, new ResultOnlyDistiller());
            var index = responseBox.lookFor(2);
            if (index >= 0) {
                streamWire.send(index, bytes);
                left.setResponseHandle(new ResponseWireHandleImpl(index));
                right.setResponseHandle(new ResponseWireHandleImpl(index));
            } else {
                queue.add(new QueueEntry<SqlResponse.ResultOnly>(bytes, left, right));
            }
            return Pair.of(left, right);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    /**
     * Receive SqlResponse.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @return ResposeProtos.Response message
     */
    @Override
    public SqlResponse.Response receive(ResponseWireHandle handle) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        try {
            byte index = ((ResponseWireHandleImpl) handle).getHandle();
            var inputStream = new ByteArrayInputStream(responseBox.receive(index));
            FrameworkResponseProtos.Header.parseDelimitedFrom(inputStream);
            var response = SqlResponse.Response.parseDelimitedFrom(inputStream);
            responseBox.release(index);
            var entry = queue.peek();
            if (!Objects.isNull(entry)) {
                if (entry.getRequestType() == RequestType.STATEMENT) {
                    var slot = responseBox.lookFor(2);
                    if (slot >= 0) {
                        streamWire.send(slot, entry.getRequest());
                        entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(slot));
                        queue.poll();
                    }
                } else {
                    var slot = responseBox.lookFor(2);
                    if (slot >= 0) {
                        streamWire.send(slot, entry.getRequest());
                        entry.getFutureHead().setResponseHandle(new ResponseWireHandleImpl(slot));
                        entry.getFutureBody().setResponseHandle(new ResponseWireHandleImpl(slot));
                        queue.poll();
                    }
                }
            }
            return response;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: SessionWireImpl.receive()", e);
        }
    }

    /**
     * Receive SqlResponse.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @return ResposeProtos.Response message
     */
    @Override
    public SqlResponse.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        try {
            return receive(handle);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException("error: SessionWireImpl.receive()", e);
        }
    }

    /**
     * UnReceive one SqlResponse.Response
     @param handle the handle to the response box
     */
    @Override
    public void unReceive(ResponseWireHandle handle) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        responseBox.unreceive(((ResponseWireHandleImpl) handle).getHandle());
    }

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     @return ResultSetWireImpl
     */
    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        return new ResultSetWireImpl(streamWire);
    }

    @Override
    public FutureResponse<? extends Response> send(long serviceID, byte[] request) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        var response = new ChannelResponse(this);
        var future = FutureResponse.wrap(Owner.of(response));
        var header = HEADER_BUILDER.setServiceId(serviceID).setSessionId(sessionID).build();
        try (var buffer = new ByteArrayOutputStream()) {
            header.writeDelimitedTo(buffer);
            var bytes = buffer.toByteArray();
            var index = responseBox.lookFor(1);
            if (index >= 0) {
                streamWire.send(index, bytes, request);
                response.setHandle(new ResponseWireHandleImpl(index));
            } else {
                throw new IOException("no response box available");  // FIXME should queueing
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
        return future;
    }

    @Override
    public FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        try {
            return send(serviceID, request.array());
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        byte index = ((ResponseWireHandleImpl) handle).getHandle();
        var byteBuffer = ByteBuffer.wrap(responseBox.receive(index));
        FrameworkResponseProtos.Header.parseDelimitedFrom(new ByteBufferInputStream(byteBuffer));
        return byteBuffer;
    }
    @Override
    public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        return response(handle);  // FIXME implement timeout
    }
}
