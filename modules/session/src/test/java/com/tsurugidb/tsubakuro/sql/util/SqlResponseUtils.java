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
import com.tsurugidb.sql.proto.SqlResponse;

/**
 * SqlResponse utilitiews.
 */
public final class SqlResponseUtils {

    private SqlResponseUtils() {
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.ResultOnly response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.ResultOnly response) {
        return SqlResponse.Response.newBuilder().setResultOnly(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.Begin response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.Begin response) {
        return SqlResponse.Response.newBuilder().setBegin(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.Prepare response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.Prepare response) {
        return SqlResponse.Response.newBuilder().setPrepare(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.ExecuteQuery response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.ExecuteQuery response) {
        return SqlResponse.Response.newBuilder().setExecuteQuery(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.Explain response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.Explain response) {
        return SqlResponse.Response.newBuilder().setExplain(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.DescribeTable response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.DescribeTable response) {
        return SqlResponse.Response.newBuilder().setDescribeTable(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.ListTables response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.ListTables response) {
        return SqlResponse.Response.newBuilder().setListTables(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.GetSearchPath response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.GetSearchPath response) {
        return SqlResponse.Response.newBuilder().setGetSearchPath(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.GetErrorInfo response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.GetErrorInfo response) {
        return SqlResponse.Response.newBuilder().setGetErrorInfo(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.DisposeTransaction response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.DisposeTransaction response) {
        return SqlResponse.Response.newBuilder().setDisposeTransaction(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.ExecuteResult response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.ExecuteResult response) {
        return SqlResponse.Response.newBuilder().setExecuteResult(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.ExtractStatementInfo response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.ExtractStatementInfo response) {
        return SqlResponse.Response.newBuilder().setExtractStatementInfo(response).build();
    }

    /**
     * Convert SqlResponse.xxx response to delimited byte[].
     * @param response the SqlResponse.xxx response
     * @return the delimited byte[]
     * @if I/O error was occurred while converting SqlResponse.xxx response
     */
    public static byte[] toSqlResponseDelimitedByteArray(@Nonnull SqlResponse.GetLargeObjectData response) {
        return toDelimitedByteArray(toSqlResponse(response));
    }
    /**
     * Set SqlResponse.xxx response into SqlResponse.Response response.
     * @param response the SqlResponse.xxx response
     * @return SqlResponse.Response response
     */
    public static SqlResponse.Response toSqlResponse(@Nonnull SqlResponse.GetLargeObjectData response) {
        return SqlResponse.Response.newBuilder().setGetLargeObjectData(response).build();
    }

    private static byte[] toDelimitedByteArray(Message response) {
        try (var buffer = new ByteArrayOutputStream()) {
            response.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }
}
