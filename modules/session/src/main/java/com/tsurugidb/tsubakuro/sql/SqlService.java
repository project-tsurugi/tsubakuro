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
package com.tsurugidb.tsubakuro.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.util.List;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * An interface to communicate with SQL service.
 */
public interface SqlService extends ServerResource {

    /**
     * Requests {@code Begin} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a transaction object.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Transaction> send(@Nonnull SqlRequest.Begin request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Commit} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull SqlRequest.Commit request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Rollback} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull SqlRequest.Rollback request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Prepare} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a prepared statement object.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<PreparedStatement> send(
            @Nonnull SqlRequest.Prepare request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code DisposePreparedStatement} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull SqlRequest.DisposePreparedStatement request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code ExplainByText} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<StatementMetadata> send(
            @Nonnull SqlRequest.ExplainByText request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Explain} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<StatementMetadata> send(
            @Nonnull SqlRequest.Explain request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code DescribeTable} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<TableMetadata> send(
            @Nonnull SqlRequest.DescribeTable request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code ExecuteStatement} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<ExecuteResult> send(@Nonnull SqlRequest.ExecuteStatement request) throws IOException {
        throw new UnsupportedOperationException();
    }
    default FutureResponse<ExecuteResult> send(@Nonnull SqlRequest.ExecutePreparedStatement request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code ExecuteStatement} to SQL service.
     * @param request the request
     * @param blobs the blobs to send
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<ExecuteResult> send(@Nonnull SqlRequest.ExecutePreparedStatement request, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Query} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a result set object which includes query results.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<ResultSet> send(@Nonnull SqlRequest.ExecuteQuery request) throws IOException {
        throw new UnsupportedOperationException();
    }
    default FutureResponse<ResultSet> send(@Nonnull SqlRequest.ExecutePreparedQuery request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Query} to SQL service.
     * @param request the request
     * @param blobs the blobs to send
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a result set object which includes query results.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<ResultSet> send(@Nonnull SqlRequest.ExecutePreparedQuery request, @Nonnull List<? extends BlobInfo> blobs) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Batch} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<ExecuteResult> send(@Nonnull SqlRequest.Batch request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code ExecuteQuery} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a result set object which includes output file paths.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<ResultSet> send(@Nonnull SqlRequest.ExecuteDump request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code ExecuteLoad} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<ExecuteResult> send(@Nonnull SqlRequest.ExecuteLoad request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code ListTables} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<TableList> send(@Nonnull SqlRequest.ListTables request) throws IOException {
                throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code SearchPath} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<SearchPath> send(@Nonnull SqlRequest.GetSearchPath request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code GetErrorInfo} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<SqlServiceException> send(@Nonnull SqlRequest.GetErrorInfo request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code GetLargeObjectData} to SQL service.
     * @param request the request
     * @param reference the blob reference
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<InputStream> send(@Nonnull SqlRequest.GetLargeObjectData request, @Nonnull BlobReference reference) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code GetLargeObjectData} to SQL service.
     * @param request the request
     * @param reference the clob reference
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Reader> send(@Nonnull SqlRequest.GetLargeObjectData request, @Nonnull ClobReference reference) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code GetLargeObjectData} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<LargeObjectCache> send(@Nonnull SqlRequest.GetLargeObjectData request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code GetLargeObjectData} to SQL service and copy the large object to a file specified by the destination.
     * @param request the request
     * @param destination the path of the destination file
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull SqlRequest.GetLargeObjectData request, @Nonnull Path destination) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code DisposeTransaction} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull SqlRequest.DisposeTransaction request) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
