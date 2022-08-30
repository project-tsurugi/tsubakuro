package com.tsurugidb.tsubakuro.common.impl;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.sql.impl.SimpleResponse;
import com.tsurugidb.tateyama.proto.CoreResponse;

/**
 * Handles request for wires and returns its response.
 */
@FunctionalInterface
public interface RequestHandler {

    /**
     * Handles a request message and returns a response for it.
     * @param serviceId the destination service ID
     * @param request the request payload
     * @return the response
     * @throws IOException if I/O error while handling the request
     * @throws ServerException if server error while handling the request
     */
    Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException;

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @param relation the resultSet
     * @return the request handler
     */
    static RequestHandler returns(ByteBuffer response, Relation relation, ByteBuffer status) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        return (id, request) -> new SimpleResponse(response, relation.getByteBuffer(), status);
    }
    static RequestHandler returns(ByteBuffer response) {
        Objects.requireNonNull(response);
        return (id, request) -> new SimpleResponse(response);
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @param relation the resultSet
     * @return the request handler
     */
    static RequestHandler returns(byte[] response, Relation relation, byte[] status) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        if (Objects.nonNull(status)) {
            return returns(ByteBuffer.wrap(response), relation, ByteBuffer.wrap(status));
        }
        return returns(ByteBuffer.wrap(response), relation, null);
    }
    static RequestHandler returns(byte[] response) {
        Objects.requireNonNull(response);
        return returns(ByteBuffer.wrap(response));
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @return the request handler
     */
    static RequestHandler returns(CoreResponse.UpdateExpirationTime response) {
        Objects.requireNonNull(response);
        return returns(toDelimitedByteArray(response));
    }

    /**
     * Creates a new request handler which throws the given exception.
     * @param exception the exception object
     * @return the request handler
     */
    static RequestHandler raises(ServerException exception) {
        Objects.requireNonNull(exception);
        return (id, request) -> {
            throw exception; 
        };
    }

    private static byte[] toDelimitedByteArray(CoreResponse.UpdateExpirationTime response) {
        try (var buffer = new ByteArrayOutputStream()) {
            response.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
