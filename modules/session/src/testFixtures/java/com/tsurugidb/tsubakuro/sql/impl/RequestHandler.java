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
package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
// import java.util.Arrays;
// import java.util.Map;
import java.util.Objects;

import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.sql.util.SqlResponseUtils;

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
     * @param metadata the metadata
     * @param relation the resultSet
     * @return the request handler
     */
    static RequestHandler returns(ByteBuffer response, ByteBuffer metadata, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        return (id, request) -> new SimpleResponse(response, metadata, relation.getByteBuffer());
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
    static RequestHandler returns(byte[] response, byte[] metadata, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        if (metadata != null) {
            return returns(ByteBuffer.wrap(response), ByteBuffer.wrap(metadata), relation);
        }
        return returns(ByteBuffer.wrap(response), null, relation);
    }
    static RequestHandler returns(byte[] response) {
        Objects.requireNonNull(response);
        return returns(ByteBuffer.wrap(response));
    }

    /**
     * Creates a new request handler which returns the given response payload.
     * @param response the response message
     * @return the request handler
     */
//    static RequestHandler returns(Message response) {
//        Objects.requireNonNull(response);
//        return returns(response.toByteArray());
//    }
    // for each response without result set transfer
    static RequestHandler returns(SqlResponse.ResultOnly response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.Begin response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.Prepare response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.Explain response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.DescribeTable response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.ListTables response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.GetSearchPath response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.GetErrorInfo response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.DisposeTransaction response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.ExecuteResult response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.ExtractStatementInfo response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.GetLargeObjectData response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }
    static RequestHandler returns(SqlResponse.GetTransactionStatus response) {
        Objects.requireNonNull(response);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response));
    }

    // for result set transfer (normal case)
    static RequestHandler returns(SqlResponse.ResultOnly response, byte[] metadata, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(metadata);
        Objects.requireNonNull(relation);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response), metadata, relation);
    }
    // for result set transfer without metadata case (error)
    static RequestHandler returns(SqlResponse.ResultOnly response, Relation relation) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(relation);
        return returns(SqlResponseUtils.toSqlResponseDelimitedByteArray(response), null, relation);
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
}
