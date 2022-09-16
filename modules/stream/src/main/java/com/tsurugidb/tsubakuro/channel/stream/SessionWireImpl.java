package com.tsurugidb.tsubakuro.channel.stream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.stream.sql.ResultSetWireImpl;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

/**
 * SessionWireImpl type.
 */
public final class SessionWireImpl implements Wire {
    private StreamWire streamWire;
    private final long sessionID;
    private final ResponseBox responseBox;

    static final Logger LOG = LoggerFactory.getLogger(SessionWireImpl.class);

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

        var header = FrameworkRequest.Header.newBuilder().setMessageVersion(1).setServiceId(serviceID).setSessionId(sessionID).build();
        var response = responseBox.register(toDelimitedByteArray(header), request);
        return FutureResponse.wrap(Owner.of(response));
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
