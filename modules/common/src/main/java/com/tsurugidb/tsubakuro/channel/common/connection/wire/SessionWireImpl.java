package com.tsurugidb.tsubakuro.channel.common.connection.wire;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

/**
 * SessionWireImpl type.
 */
public final class SessionWireImpl implements Wire {

    static final Logger LOG = LoggerFactory.getLogger(SessionWireImpl.class);

    private Link link;
    private final long sessionID;
    private final ResponseBox responseBox;

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param link the stream object by which this SessionWireImpl is connected to the SQL server
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public SessionWireImpl(@Nonnull Link link, long sessionID) throws IOException {
        this.link = link;
        this.sessionID = sessionID;
        this.responseBox = link.getResponseBox();
        link.start();
        LOG.trace("begin Session via ipc, id = {}", sessionID);
    }

    /**
     * Send a Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in byte[]
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in ByteBuffer variant of send()
     */
    @Override
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload) throws IOException {
        if (Objects.isNull(link)) {
            throw new IOException("already closed");
        }
        var header = FrameworkRequest.Header.newBuilder().setMessageVersion(1).setServiceId(serviceId).setSessionId(sessionID).build();
        var response = responseBox.register(toDelimitedByteArray(header), payload);
        return FutureResponse.wrap(Owner.of(response));
    }

    /**
     * Send a Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in ByteBuffer
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in responseBox.register()
     */
    @Override
    public  FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload) throws IOException {
        return send(serviceId, payload.array());
    }

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return ResultSetWireImpl
    */
    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        if (Objects.isNull(link)) {
            throw new IOException("already closed");
        }
        return link.createResultSetWire();
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        try {
            if (Objects.nonNull(link)) {
                link.close();
                link = null;
            }
        } catch (ServerException | InterruptedException e) {
            throw new IOException(e);
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
