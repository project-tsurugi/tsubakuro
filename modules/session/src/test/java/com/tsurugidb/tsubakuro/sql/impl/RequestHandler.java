package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
// import java.util.Arrays;
// import java.util.Map;
import java.util.Objects;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.sql.proto.SqlResponse;

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
//    static RequestHandler returns(Message response) {
//        Objects.requireNonNull(response);
//        return returns(response.toByteArray());
//    }

    static RequestHandler returns(SqlResponse.ResultOnly response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setResultOnly(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandler returns(SqlResponse.Begin response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setBegin(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandler returns(SqlResponse.Prepare response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setPrepare(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandler returns(SqlResponse.Explain response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setExplain(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandler returns(SqlResponse.DescribeTable response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setDescribeTable(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandler returns(SqlResponse.Batch response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setBatch(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }

    static RequestHandler returns(SqlResponse.ExecuteQuery response, Relation relation, SqlResponse.ResultOnly status) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setExecuteQuery(response).build();
        var sqlStatus = SqlResponse.Response.newBuilder().setResultOnly(status).build();
        return returns(toDelimitedByteArray(sqlResponse), relation, toDelimitedByteArray(sqlStatus));
    }
    static RequestHandler returns(SqlResponse.ResultOnly response, Relation relation) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setResultOnly(response).build();
        return returns(toDelimitedByteArray(sqlResponse), relation, null);
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

    private static byte[] toDelimitedByteArray(SqlResponse.Response response) {
        try (var buffer = new ByteArrayOutputStream()) {
            response.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
