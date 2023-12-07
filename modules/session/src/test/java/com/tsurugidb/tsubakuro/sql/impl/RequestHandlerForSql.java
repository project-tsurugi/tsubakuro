package com.tsurugidb.tsubakuro.sql.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
// import java.util.Arrays;
// import java.util.Map;
import java.util.Objects;

import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.mock.RequestHandler;

/**
 * Handles request for wires and returns its response.
 */
@FunctionalInterface
public interface RequestHandlerForSql extends RequestHandler {

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
     * @param metadata the metadata
     * @param relation the resultSet
     * @return the request handler
     */
    static RequestHandlerForSql returns(ByteBuffer response, ByteBuffer metadata, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        return (id, request) -> new SimpleResponse(response, metadata, relation.getByteBuffer());
    }
    static RequestHandlerForSql returns(ByteBuffer response) {
        Objects.requireNonNull(response);
        return (id, request) -> new SimpleResponse(response);
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @param relation the resultSet
     * @return the request handler
     */
    static RequestHandlerForSql returns(byte[] response, byte[] metadata, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        if (metadata != null) {
            return returns(ByteBuffer.wrap(response), ByteBuffer.wrap(metadata), relation);
        }
        return returns(ByteBuffer.wrap(response), null, relation);
    }
    static RequestHandlerForSql returns(byte[] response) {
        Objects.requireNonNull(response);
        return returns(ByteBuffer.wrap(response));
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response payload
     * @return the request handler
     */
//    static RequestHandlerForSql returns(Message response) {
//        Objects.requireNonNull(response);
//        return returns(response.toByteArray());
//    }
    // for each response without result set transfer
    static RequestHandlerForSql returns(SqlResponse.ResultOnly response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setResultOnly(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.Begin response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setBegin(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.Prepare response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setPrepare(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.Explain response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setExplain(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.DescribeTable response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setDescribeTable(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.ExecuteResult response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setExecuteResult(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.Batch response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setBatch(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.ListTables response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setListTables(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.GetSearchPath response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setGetSearchPath(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.GetErrorInfo response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setGetErrorInfo(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }
    static RequestHandlerForSql returns(SqlResponse.DisposeTransaction response) {
        Objects.requireNonNull(response);
        var sqlResponse = SqlResponse.Response.newBuilder().setDisposeTransaction(response).build();
        return returns(toDelimitedByteArray(sqlResponse));
    }

    // for result set transfer (normal case)
    static RequestHandlerForSql returns(SqlResponse.ResultOnly response, byte[] metadata, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(metadata);
        Objects.requireNonNull(relation);
        var status = SqlResponse.Response.newBuilder().setResultOnly(response).build();
        return returns(toDelimitedByteArray(status), metadata, relation);
    }
    // for result set transfer without metadata case (error)
    static RequestHandlerForSql returns(SqlResponse.ResultOnly response, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        var sqlResponse = SqlResponse.Response.newBuilder().setResultOnly(response).build();
        return returns(toDelimitedByteArray(sqlResponse), null, relation);
    }

    /**
     * Creates a new request handler which throws the given exception.
     * @param exception the exception object
     * @return the request handler
     */
    static RequestHandlerForSql raises(ServerException exception) {
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
            System.err.println(e);
            e.printStackTrace();
        }
        return null;
    }
}
