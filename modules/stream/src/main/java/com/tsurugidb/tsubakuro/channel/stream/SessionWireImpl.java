package com.tsurugidb.tsubakuro.channel.stream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseWireHandle;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetWireImpl;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResponseBox;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.Owner;

/**
 * SessionWireImpl type.
 */
public final class SessionWireImpl implements Wire {
    static final FrameworkRequest.Header.Builder HEADER_BUILDER = FrameworkRequest.Header.newBuilder().setMessageVersion(1);

    private StreamWire streamWire;
    private final long sessionID;
    private final ResponseBox responseBox;
    private final Queue<QueueEntry> queue = new ArrayDeque<>();

    static final Logger LOG = LoggerFactory.getLogger(SessionWireImpl.class);

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
     * @param streamWire the stream object by which this SessionWireImpl is connected to the SQL server
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     */
    public SessionWireImpl(StreamWire streamWire, long sessionID) {
        this.streamWire = streamWire;
        this.sessionID = sessionID;
        this.responseBox = streamWire.getResponseBox();
        this.streamWire.start();
        LOG.trace("begin Session via stream, id = " + sessionID);
    }

    @Override
    public FutureResponse<? extends Response> send(int serviceID, byte[] request) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        var response = new ChannelResponse(this);
        var future = FutureResponse.wrap(Owner.of(response));
        synchronized (this) {
            var header = HEADER_BUILDER.setServiceId(serviceID).setSessionId(sessionID).build();
            var slot = responseBox.lookFor();
            if (slot >= 0) {
                response.setResponseHandle(new ResponseWireHandleImpl(slot));
                streamWire.send(slot, toDelimitedByteArray(header), request);
                LOG.trace("send {}, slot = {}", request, slot);
            } else {
                queue.add(new QueueEntry(serviceID, request, response));
            }
        }
        return future;
    }

    @Override
    public FutureResponse<? extends Response> send(int serviceID, ByteBuffer request) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        try {
            return send(serviceID, request.array());  // FIXME in case of Direct ByteBuffer
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle) throws IOException {
        if (Objects.isNull(streamWire)) {
            throw new IOException("already closed");
        }
        byte slot = ((ResponseWireHandleImpl) handle).getHandle();
        var byteBuffer = ByteBuffer.wrap(responseBox.receive(slot));
        FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(byteBuffer));

        return byteBuffer;
    }
    @Override
    public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        return response(handle);  // FIXME implement timeout
    }

    /**
     * Set to receive a Query type response by response box
     */
    @Override
    public void setResultSetMode(ResponseWireHandle handle) {
        responseBox.setResultSetMode(((ResponseWireHandleImpl) handle).getHandle());
    }

    /**
     * release the message in the response box
     * @param handle the handle to the response box
    */
    @Override
    public void release(ResponseWireHandle handle) throws IOException {
        responseBox.release(((ResponseWireHandleImpl) handle).getHandle());

        synchronized (this) {
            var entry = queue.peek();
            if (Objects.nonNull(entry)) {
                var nextSlot = responseBox.lookFor();
                if (nextSlot >= 0) {
                    entry.getResponse().setResponseHandle(new ResponseWireHandleImpl(nextSlot));

                    var header = HEADER_BUILDER.setServiceId(entry.serviceId()).setSessionId(sessionID).build();
                    streamWire.send(nextSlot, toDelimitedByteArray(header), entry.getRequest());
                    queue.poll();
                    LOG.trace("send {}, slot = {}", entry.getRequest(), nextSlot);
                }
            }
        }
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

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        if (Objects.nonNull(streamWire)) {
            streamWire.close();
            streamWire = null;
        }
    }

    byte[] toDelimitedByteArray(FrameworkRequest.Header request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
}
