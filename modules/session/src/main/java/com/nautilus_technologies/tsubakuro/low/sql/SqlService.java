package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

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
     * Requests {@code Explain} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<String> send(
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
    default FutureResponse<Void> send(@Nonnull SqlRequest.ExecuteStatement request) throws IOException {
        throw new UnsupportedOperationException();
    }
    default FutureResponse<Void> send(@Nonnull SqlRequest.ExecutePreparedStatement request) throws IOException {
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
     * Requests {@code Batch} to SQL service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull SqlRequest.Batch request) throws IOException {
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
    default FutureResponse<Void> send(@Nonnull SqlRequest.ExecuteLoad request) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests {@code Disconnect} to SQL service, for compatibility.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull SqlRequest.Disconnect request) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
