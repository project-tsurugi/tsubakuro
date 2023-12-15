package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.ForegroundFutureResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * WireImpl type.
 */
public class WireImpl implements Wire {
    /**
     * The major service message version for FrameworkRequest.Header.
     */
    private static final int SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version for FrameworkRequest.Header.
     */
    private static final int SERVICE_MESSAGE_VERSION_MINOR = 0;

    private static final int SERVICE_ID_ENDPOINT_BROKER = 1;
    private static final long SESSION_ID_IS_NOT_ASSIGNED = Long.MAX_VALUE;

    static final Logger LOG = LoggerFactory.getLogger(WireImpl.class);

    private final Link link;
    private final ResponseBox responseBox;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long sessionID;

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param link the stream object by which this WireImpl is connected to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public WireImpl(@Nonnull Link link) throws IOException {
        this.link = link;
        this.responseBox = link.getResponseBox();
        this.sessionID = SESSION_ID_IS_NOT_ASSIGNED;
        LOG.trace("begin Session via ipc, id = {}", sessionID);
    }

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param link the stream object by which this WireImpl is connected to the SQL server
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public WireImpl(@Nonnull Link link, long sessionID) throws IOException {
        this.link = link;
        this.sessionID = sessionID;
        this.responseBox = link.getResponseBox();
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
        if (closed.get()) {
            throw new IOException("already closed");
        }
        var header = FrameworkRequest.Header.newBuilder()
            .setServiceMessageVersionMajor(SERVICE_MESSAGE_VERSION_MAJOR)
            .setServiceMessageVersionMinor(SERVICE_MESSAGE_VERSION_MINOR)
            .setServiceId(serviceId)
            .setSessionId(sessionID)
            .build();
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
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload) throws IOException {
        return send(serviceId, payload.array());
    }

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return ResultSetWireImpl
    */
    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        if (closed.get()) {
            throw new IOException("already closed");
        }
        return link.createResultSetWire();
    }

    @Override
    public boolean isAlive() {
        if (closed.get()) {
            return false;
        }
        return link.isAlive();
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        link.setCloseTimeout(timeout);
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        try {
            if (!closed.get()) {
                link.close();
                closed.set(true);
            }
        } catch (ServerException | InterruptedException e) {
            throw new IOException(e);
        }

    }

    private static EndpointRequest.Request.Builder newRequest() {
        return EndpointRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(SERVICE_MESSAGE_VERSION_MINOR);
    }

    static class HandshakeProcessor implements MainResponseProcessor<Long> {
        @Override
        public Long process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = EndpointResponse.Handshake.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return message.getSuccess().getSessionId();

            case ERROR:
                var errMessage = message.getError();
                switch (errMessage.getCode()) {
                case RESOURCE_LIMIT_REACHED:
                    throw new ConnectException("the server has declined the connection request");  // preserve compatibiity
                case AUTHENTICATION_ERROR:
                    throw newUnknown(message.getError());  // FIXME
                default:
                    break;
                }
            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    public FutureResponse<Long> handshake(@Nonnull Credential credential, @Nonnull ClientInformation clientInformation) throws IOException {
        var handshakeMessageBuilder = EndpointRequest.Handshake.newBuilder();
//        if (credential != NullCredential.INSTANCE) {
//            handshakeMessageBuilder.setAuthInfo(credential);  // FIXME
//        }
        handshakeMessageBuilder.setClientInformation(EndpointRequest.ClientInformation.newBuilder()
                    .setConnectionLabel(clientInformation.connectionLabel())
                    .setApplicationName(clientInformation.applicationName())
                    .setUserName(clientInformation.userName())
                    .setConnectionInformation(clientInformation.connectionInformation())
                    .setMaximumConcurrentResultSets(clientInformation.maximumConcurrentResultSets()));
        FutureResponse<? extends Response> future = send(
            SERVICE_ID_ENDPOINT_BROKER,
                toDelimitedByteArray(newRequest()
                    .setHandshake(handshakeMessageBuilder)
                    .build())
            );
        return new ForegroundFutureResponse<>(future, new HandshakeProcessor().asResponseProcessor());
    }

    public void setSessionID(long id) throws IOException {
        if (sessionID == SESSION_ID_IS_NOT_ASSIGNED) {
            this.sessionID = id;
            return;
        }
        throw new IOException("handshake error (session ID is already assigned)");
    }

    public void checkSessionID(long id) throws IOException {
        if (sessionID != id) {
            throw new IOException(MessageFormat.format("handshake error (inconsistent session ID), {0} not equal {1}", sessionID, id));
        }
    }

    byte[] toDelimitedByteArray(FrameworkRequest.Header request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }

    byte[] toDelimitedByteArray(EndpointRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }

    static CoreServiceException newUnknown(@Nonnull EndpointResponse.Error message) {
        assert message != null;
        return new CoreServiceException(CoreServiceCode.UNKNOWN, message.getMessage());
    }

    // for diagnostic
    public long sessionID() {
        return sessionID;
    }
    @Override
    public String diagnosticInfo() {
        String diagnosticInfo = responseBox.diagnosticInfo();
        if (!diagnosticInfo.isEmpty()) {
            String rv = " +Requests in processing" + System.getProperty("line.separator");
            rv += diagnosticInfo;
            return rv;
        }
        return "";
    }
}
