package com.nautilus_technologies.tsubakuro.channel.stream;

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
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.stream.sql.ResultSetWireImpl;
import com.nautilus_technologies.tsubakuro.channel.stream.sql.ResponseBox;
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

    private StreamWire streamWire;
    private final long sessionID;
    private final ResponseBox responseBox;
    private final Queue<QueueEntry> queue;

    static class QueueEntry {
        final long serviceId;
        final byte[] request;

        QueueEntry(long serviceId, byte[] request) {
            this.serviceId = serviceId;
            this.request = request;
        }
        long serviceId() {
            return serviceId;
        }
        byte[] getRequest() {
            return request;
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
            var slot = responseBox.lookFor();
            if (slot >= 0) {
                streamWire.send(slot, bytes, request);
                response.setResponseHandle(new ResponseWireHandleImpl(slot));
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
        byte slot = ((ResponseWireHandleImpl) handle).getHandle();
        var byteBuffer = ByteBuffer.wrap(responseBox.receive(slot));
        responseBox.release(slot);
        FrameworkResponseProtos.Header.parseDelimitedFrom(new ByteBufferInputStream(byteBuffer));
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
    public void setQueryMode(ResponseWireHandle handle) {
        responseBox.setQueryMode(((ResponseWireHandleImpl) handle).getHandle());
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

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        streamWire.close();
        streamWire = null;
    }
}
