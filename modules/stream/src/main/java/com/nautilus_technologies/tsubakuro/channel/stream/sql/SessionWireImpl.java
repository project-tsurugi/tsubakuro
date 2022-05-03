package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.sql.FutureQueryResponseImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.FutureResponseImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.ResultOnlyDistiller;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;

/**
 * SessionWireImpl type.
 */
public class SessionWireImpl implements SessionWire {
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
     * Send RequestProtos.Request to the SQL server via the native wire.
     @param request the RequestProtos.Request message
     @return a FutureResponse response message corresponding the request
     */
    @Override
    public <V> FutureResponse<V> send(RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        var req = request.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(sessionID)).build().toByteArray();
        var futureBody = new FutureResponseImpl<V>(this, distiller);
        var index = responseBox.lookFor(1);
        if (index >= 0) {
            streamWire.send(index, req);
            futureBody.setResponseHandle(new ResponseWireHandleImpl(index));
        } else {
            queue.add(new QueueEntry<V>(req, futureBody));
        }
        return futureBody;
    }

    /**
     * Send RequestProtos.Request to the SQL server via the native wire.
     @param request the RequestProtos.Request message
     @return a couple of FutureResponse response message corresponding the request
     */
    @Override
    public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> sendQuery(RequestProtos.Request.Builder request) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        var req = request.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(sessionID)).build().toByteArray();
        var left = new FutureQueryResponseImpl(this);
        var right = new FutureResponseImpl<ResponseProtos.ResultOnly>(this, new ResultOnlyDistiller());
        var index = responseBox.lookFor(2);
        if (index >= 0) {
            streamWire.send(index, req);
            left.setResponseHandle(new ResponseWireHandleImpl(index));
            right.setResponseHandle(new ResponseWireHandleImpl(index));
        } else {
            queue.add(new QueueEntry<ResponseProtos.ResultOnly>(req, left, right));
        }
        return Pair.of(left, right);
    }

    /**
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @return ResposeProtos.Response message
     */
    @Override
    public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        try {
            byte index = ((ResponseWireHandleImpl) handle).getHandle();

            var response = ResponseProtos.Response.parseFrom(responseBox.receive(index));
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
     * Receive ResponseProtos.Response from the SQL server via the native wire.
     @param handle the handle indicating the sent request message corresponding to the response message to be received.
     @return ResposeProtos.Response message
     */
    @Override
    public ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
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
     * UnReceive one ResponseProtos.Response
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
}
