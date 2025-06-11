/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.sql.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.protobuf.Message;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * SqlRequest utilitiews.
 */
public final class SqlRequestUtils {

    private SqlRequestUtils() {
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.Begin request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.Begin request) {
        return newRequest().setBegin(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.Commit request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.Commit request) {
        return newRequest().setCommit(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.Rollback request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.Rollback request) {
        return newRequest().setRollback(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.Prepare request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.Prepare request) {
        return newRequest().setPrepare(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.DisposePreparedStatement request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.DisposePreparedStatement request) {
        return newRequest().setDisposePreparedStatement(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.Explain request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.Explain request) {
        return newRequest().setExplain(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExplainByText request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExplainByText request) {
        return newRequest().setExplainByText(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.DescribeTable request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.DescribeTable request) {
        return newRequest().setDescribeTable(request).build();
    }


    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExecuteStatement request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExecuteStatement request) {
        return newRequest().setExecuteStatement(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExecutePreparedStatement request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExecutePreparedStatement request) {
        return newRequest().setExecutePreparedStatement(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.Batch request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.Batch request) {
        return newRequest().setBatch(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExecuteQuery request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExecuteQuery request) {
        return newRequest().setExecuteQuery(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExecutePreparedQuery request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExecutePreparedQuery request) {
        return newRequest().setExecutePreparedQuery(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExecuteDump request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExecuteDump request) {
        return newRequest().setExecuteDump(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExecuteDumpByText request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExecuteDumpByText request) {
        return newRequest().setExecuteDumpByText(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ExecuteLoad request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ExecuteLoad request) {
        return newRequest().setExecuteLoad(request).build();
    }

        /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.ListTables request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.ListTables request) {
        return newRequest().setListTables(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.GetSearchPath request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.GetSearchPath request) {
        return newRequest().setGetSearchPath(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.GetErrorInfo request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.GetErrorInfo request) {
        return newRequest().setGetErrorInfo(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.GetLargeObjectData request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.GetLargeObjectData request) {
        return newRequest().setGetLargeObjectData(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.GetTransactionStatus request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.GetTransactionStatus request) {
        return newRequest().setGetTransactionStatus(request).build();
    }

    /**
     * Convert SqlRequest.xxx request to delimited byte[].
     * @param request the SqlRequest.xxx request
     * @return the delimited byte[]
     * @throws IOException if I/O error was occurred while converting SqlRequest.xxx request
     */
    public static byte[] toSqlRequestDelimitedByteArray(@Nonnull SqlRequest.DisposeTransaction request) throws IOException {
        return toDelimitedByteArray(toSqlRequest(request));
    }
    /**
     * Set SqlRequest.xxx request into SqlRequest.Request request.
     * @param request the SqlRequest.xxx request
     * @return SqlRequest.Request request
     */
    public static SqlRequest.Request toSqlRequest(@Nonnull SqlRequest.DisposeTransaction request) {
        return newRequest().setDisposeTransaction(request).build();
    }


    private static SqlRequest.Request.Builder newRequest() {
        return SqlRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(SqlClient.SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(SqlClient.SERVICE_MESSAGE_VERSION_MINOR);
    }
    private static byte[] toDelimitedByteArray(Message request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }
}
