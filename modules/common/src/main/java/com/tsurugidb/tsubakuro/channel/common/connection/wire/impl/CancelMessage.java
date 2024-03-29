package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.protobuf.Message;

import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.endpoint.proto.EndpointRequest;

final class CancelMessage {
    /**
     * The major service message version for FrameworkRequest.Header.
     */
    private static final int SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version for FrameworkRequest.Header.
     */
    private static final int SERVICE_MESSAGE_VERSION_MINOR = 0;

    /**
     * The major service message version for EndpointRequest.Request.
     */
    private static final int ENDPOINT_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version for EndpointRequest.Request.
     */
    private static final int ENDPOINT_MESSAGE_VERSION_MINOR = 0;

    /**
     * The service id for endpoint broker.
     */
    private static final int SERVICE_ID_ENDPOINT_BROKER = 1;

    private static final FrameworkRequest.Header.Builder HEADER;
    private static final EndpointRequest.Request REQUEST;

    static {
        HEADER = FrameworkRequest.Header.newBuilder()
            .setServiceMessageVersionMajor(SERVICE_MESSAGE_VERSION_MAJOR)
            .setServiceMessageVersionMinor(SERVICE_MESSAGE_VERSION_MINOR)
            .setServiceId(SERVICE_ID_ENDPOINT_BROKER);
        REQUEST = EndpointRequest.Request.newBuilder()
            .setServiceMessageVersionMajor(ENDPOINT_MESSAGE_VERSION_MAJOR)
            .setServiceMessageVersionMinor(ENDPOINT_MESSAGE_VERSION_MINOR)
            .setCancel(EndpointRequest.Cancel.newBuilder())
            .build();
    }

    private CancelMessage() {
    }

    static byte[] header(long sessionId) throws IOException {
        return toDelimitedByteArray(HEADER.setSessionId(sessionId).build());
    }

    static byte[] payload() throws IOException {
        return toDelimitedByteArray(REQUEST);
    }

    private static byte[] toDelimitedByteArray(Message message) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            message.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }
}
