/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkCommon;
import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.endpoint.proto.EndpointResponse;
import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.common.BlobPathMapping;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
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
    private static final int SERVICE_MESSAGE_VERSION_MINOR = 1;

    /**
     * The major service message version for EndpointRequest.
     */
    private static final int ENDPOINT_BROKER_SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version for EndpointRequest.
     */
    private static final int ENDPOINT_BROKER_SERVICE_MESSAGE_VERSION_MINOR = 0;

    /**
     * The service id for endpoint broker.
     */
    private static final int SERVICE_ID_ENDPOINT_BROKER = 1;

    /**
     * The maximum timeout in days.
     */
    public static final long MAX_TIMEOUT_DAYS = (10 * 365);

    static final Logger LOG = LoggerFactory.getLogger(WireImpl.class);

    private final Link link;
    private final AtomicBoolean closed = new AtomicBoolean();
    private BlobPathMapping blobPathMapping = null;
    
    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param link the stream object by which this WireImpl is connected to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public WireImpl(@Nonnull Link link) throws IOException {
        this.link = link;
        LOG.trace("begin Session");
    }

    /**
     * Set BlobPathMapping.
     * @param mapping path mapping used when passing blobs to the server
     */
    public void setBlobPathMapping(BlobPathMapping mapping) {
        this.blobPathMapping = mapping;
    }

    @Override
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload) throws IOException {
        if (closed.get()) {
            throw new IOException("already closed");
        }
        var header = FrameworkRequest.Header.newBuilder()
            .setServiceMessageVersionMajor(SERVICE_MESSAGE_VERSION_MAJOR)
            .setServiceMessageVersionMinor(SERVICE_MESSAGE_VERSION_MINOR)
            .setServiceId(serviceId)
            .setSessionId(sessionId())
            .build();
        var response = link.send(toDelimitedByteArray(header), payload);
        return FutureResponse.wrap(Owner.of(response));
    }

    /**
     * Send a Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in ByteBuffer
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in link.send()
     */
    @Override
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload) throws IOException {
        return send(serviceId, payload.array());
    }

    @Override
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        if (closed.get()) {
            throw new IOException("already closed");
        }
        var header = FrameworkRequest.Header.newBuilder()
            .setServiceMessageVersionMajor(SERVICE_MESSAGE_VERSION_MAJOR)
            .setServiceMessageVersionMinor(SERVICE_MESSAGE_VERSION_MINOR)
            .setServiceId(serviceId)
            .setSessionId(sessionId());
        if (!blobs.isEmpty()) {
            var repeatedBlobInfo = FrameworkCommon.RepeatedBlobInfo.newBuilder();
            HashSet<String> dupCheck = new HashSet<>();
            for (var e: blobs) {
                if (!dupCheck.add(e.getChannelName())) {
                    throw new IllegalArgumentException("duplicate channel name: " + e.getChannelName());
                }
                var blobInfo = FrameworkCommon.BlobInfo.newBuilder().setChannelName(e.getChannelName());
                if (e.getPath().isPresent() && e.isFile()) {
                    blobInfo.setPath(serverPathString(e.getPath().get()));
                }
                repeatedBlobInfo.addBlobs(blobInfo.build());
            }
            header.setBlobs(repeatedBlobInfo);
        }
        var response = link.send(toDelimitedByteArray(header.build()), payload);
        return FutureResponse.wrap(Owner.of(response));
    }

    private String serverPathString(Path blobPath) {
        if (blobPathMapping == null) {
            return blobPath.toString();
        }
        var mapping = blobPathMapping.getOnSend();
        for (var entry : mapping) {
            var cp = entry.getClientPath();
            var clientPath = cp.isAbsolute() ? cp : cp.toAbsolutePath();
            if (blobPath.startsWith(clientPath)) {
                var remainingPath = blobPath.subpath(entry.getClientPath().getNameCount(), blobPath.getNameCount());
                if (remainingPath != null) {
                    String serverPath = entry.getServerPath();
                    for (int i = 0; i < remainingPath.getNameCount(); i++) {
                        serverPath += "/" + remainingPath.getName(i).toString();  // server path is separated by "/"
                    }
                    return serverPath;
                }
            }
        }
        return blobPath.toString();
    }

    @Override
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        return send(serviceId, payload.array(), blobs);
    }

    /**
     * Send an Urgent Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in byte[]
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in ByteBuffer variant of send()
     */
    public FutureResponse<? extends Response> sendUrgent(int serviceId, @Nonnull byte[] payload) throws IOException {
        if (closed.get()) {
            throw new IOException("already closed");
        }
        var header = FrameworkRequest.Header.newBuilder()
            .setServiceMessageVersionMajor(SERVICE_MESSAGE_VERSION_MAJOR)
            .setServiceMessageVersionMinor(SERVICE_MESSAGE_VERSION_MINOR)
            .setServiceId(serviceId)
            .setSessionId(sessionId())
            .build();
        var response = link.sendUrgent(toDelimitedByteArray(header), payload);
        return FutureResponse.wrap(Owner.of(response));
    }

    /**
     * Send an Urgent Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in ByteBuffer
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in link.send()
     */
    public FutureResponse<? extends Response> sendUrgent(int serviceId, @Nonnull ByteBuffer payload) throws IOException {
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
    public void close() throws IOException, InterruptedException, ServerException {
        if (!closed.getAndSet(true)) {
            link.close();
        }
    }

    /**
     * Set wire to close state without link close.
     * The link must be closed for this method to be called.
     */
    public void closeWithoutGet() {
        closed.set(true);
    }

    private static EndpointRequest.Request.Builder newRequest() {
        return EndpointRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(ENDPOINT_BROKER_SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(ENDPOINT_BROKER_SERVICE_MESSAGE_VERSION_MINOR);
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
                    throw newUnknown();  // FIXME
                default:
                    break;
                }
            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    public FutureResponse<Long> handshake(@Nonnull ClientInformation clientInformation, @Nullable EndpointRequest.WireInformation wireInformation) throws IOException {
        var handshakeMessageBuilder = EndpointRequest.Handshake.newBuilder();
        if (wireInformation != null) {
            handshakeMessageBuilder.setWireInformation(wireInformation);
        }

        var clientInformationBuilder = EndpointRequest.ClientInformation.newBuilder();
        if (clientInformation.getConnectionLabel() != null) {
            clientInformationBuilder.setConnectionLabel(clientInformation.getConnectionLabel());
        }
        if (clientInformation.getApplicationName() != null) {
            clientInformationBuilder.setApplicationName(clientInformation.getApplicationName());
        }
        handshakeMessageBuilder.setClientInformation(clientInformationBuilder);

        FutureResponse<? extends Response> future = send(
            SERVICE_ID_ENDPOINT_BROKER,
                toDelimitedByteArray(newRequest()
                    .setHandshake(handshakeMessageBuilder)
                    .build())
            );
        return new ForegroundFutureResponse<>(future, new HandshakeProcessor().asResponseProcessor(), null);
    }

    public void checkSessionId(long id) throws IOException {
        if (sessionId() != id) {
            throw new IOException(MessageFormat.format("handshake error (inconsistent session ID), {0} not equal {1}", sessionId(), id));
        }
    }

    static byte[] toDelimitedByteArray(Message request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }

    static CoreServiceException newUnknown(@Nonnull EndpointResponse.Error message) {
        assert message != null;
        return new CoreServiceException(CoreServiceCode.UNKNOWN, message.getMessage());
    }

    static CoreServiceException newUnknown() {
        return new CoreServiceException(CoreServiceCode.UNKNOWN);
    }

    // for diagnostic
    public long sessionId() {
        return link.sessionId();
    }
    @Override
    public String diagnosticInfo() {
        String diagnosticInfo = link.diagnosticInfo();
        if (!diagnosticInfo.isEmpty()) {
            String rv = " +Requests in processing" + System.getProperty("line.separator");
            rv += diagnosticInfo;
            return rv;
        }
        return "";
    }
}
