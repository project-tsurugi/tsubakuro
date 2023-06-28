package com.tsurugidb.tsubakuro.kvs.impl;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsTransaction;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.BatchResult;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.PutResult;
import com.tsurugidb.tsubakuro.kvs.RecordCursor;
import com.tsurugidb.tsubakuro.kvs.RemoveResult;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * An interface to communicate Tsurugi KVS service.
 */
public interface KvsService extends ServerResource {

    /**
     * Converts transaction handle which issued by this service, to transport style data.
     * @param handle the source transaction handle
     * @return the corresponding handle data
     * @throws IllegalArgumentException if the transaction handle is not supported
     */
    default KvsTransaction.Handle extract(@Nonnull TransactionHandle handle) {
        throw new UnsupportedOperationException(String.valueOf(handle));
    }

    /**
     * Requests {@code Begin} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a transaction handle object
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<TransactionHandle> send(@Nonnull KvsRequest.Begin request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests {@code Commit} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull KvsRequest.Commit request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests {@code Rollback} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull KvsRequest.Rollback request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests {@code CloseTransaction} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<Void> send(@Nonnull KvsRequest.CloseTransaction request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }


    /**
     * Requests {@code Get} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns an operation result object
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<GetResult> send(@Nonnull KvsRequest.Get request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests {@code Put} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns an operation result object
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<PutResult> send(@Nonnull KvsRequest.Put request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests {@code Remove} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns an operation result object
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<RemoveResult> send(@Nonnull KvsRequest.Remove request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests {@code Scan} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns a cursor of scan range
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<RecordCursor> send(@Nonnull KvsRequest.Scan request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests {@code Batch} to KVS service.
     * @param request the request
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns an operation result object
     * @throws IOException if I/O error was occurred while sending the request
     */
    default FutureResponse<BatchResult> send(@Nonnull KvsRequest.Batch request) throws IOException {
        throw new UnsupportedOperationException(String.valueOf(request));
    }

    /**
     * Requests empty message to KVS service.
     * @return the future response of the request,
     *      which may raise error if the request was failed.
     *      If the request was succeeded, future will returns an operation result object
     * @throws IOException if I/O error was occurred while sending the request
     * @note this method is designed just only for benchmark or debug.
     */
    default FutureResponse<Void> request() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes this service.
     * <p>
     * This operation may close underlying resources of this service (e.g. transaction handle).
     * </p>
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
