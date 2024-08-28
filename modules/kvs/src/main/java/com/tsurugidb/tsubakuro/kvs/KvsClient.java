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
package com.tsurugidb.tsubakuro.kvs;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.tsubakuro.client.ServiceClient;
import com.tsurugidb.tsubakuro.client.ServiceMessageVersion;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.impl.KvsClientImpl;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A KVS service client.
 * <p>
 * The most operations will return {@link FutureResponse future response} of the operation result.
 * You can obtain the result to invoke {@link FutureResponse#await()}, but may raise an error represented by
 * {@link KvsServiceException}.
 * </p>
 */
@ServiceMessageVersion(
        service = KvsClient.SERVICE_SYMBOLIC_ID,
        major = KvsClient.SERVICE_MESSAGE_VERSION_MAJOR,
        minor = KvsClient.SERVICE_MESSAGE_VERSION_MINOR)
public interface KvsClient extends ServerResource, ServiceClient {

    /**
     * The symbolic ID of the destination service.
    */
    String SERVICE_SYMBOLIC_ID = "kvs";

    /**
     * The major service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MAJOR = 0;

    /**
     * The minor service message version which this client requests.
     */
    int SERVICE_MESSAGE_VERSION_MINOR = 0;

    /**
     * Attaches to the SQL service in the current session.
     * @param session the current session
     * @return the SQL service client
     */
    static KvsClient attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return KvsClientImpl.attach(session);
    }

    /**
     * Starts a new transaction with default transaction options.
     * @return a future response of transaction object
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<TransactionHandle> beginTransaction() throws IOException {
        return beginTransaction(new TransactionOption());
    }

    /**
     * Starts a new transaction.
     * @param option the transaction option
     * @return a future response of transaction object
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<TransactionHandle> beginTransaction(@Nonnull TransactionOption option) throws IOException;

    /**
     * Commits the transaction.
     * <p>
     * This is equivalent to {@code #commit(TransactionHandle, CommitType) commit(transaction, CommitType.DEFAULT_BEHAVIOR}.
     * </p>
     * @param transaction the target transaction handle
     * @return a future response of this action:
     *      the response will be returned after the transaction will reach the commit status,
     *      or raise error if the commit operation was failed
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     * @see #commit(TransactionHandle, CommitType)
     * @see CommitType#DEFAULT_BEHAVIOR
     */
    default FutureResponse<Void> commit(@Nonnull TransactionHandle transaction) throws IOException {
        return commit(transaction, CommitType.DEFAULT_BEHAVIOR);
    }

    /**
     * Commits the transaction.
     * @param transaction the target transaction handle
     * @param behavior the operation behavior
     * @return a future response of this action:
     *      the response will be returned after the transaction will reach the commit status,
     *      or raise error if the commit operation was failed
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<Void> commit(
            @Nonnull TransactionHandle transaction, @Nonnull CommitType behavior) throws IOException;

    /**
     * Requests roll-back transaction.
     * @param transaction the target transaction handle
     * @return a future response of this action.
     *      Even if the transaction is already committed or rolled-back, the future will not raise any errors.
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<Void> rollback(@Nonnull TransactionHandle transaction) throws IOException;

    /**
     * Requests {@code GET} operation, which retrieves records on the table.
     * <p>
     * This does not raise errors even if there is no such the record for the key.
     * You can check it use {@link GetResult#isEmpty()}.
     * </p>
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param transaction the context transaction handle
     * @param table the source table
     * @param key the index key of the target table
     * @return a future response of this action.
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<GetResult> get(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer key) throws IOException;

    /**
     * Requests {@code PUT} operation, which creates or updates a row on the table.
     * <p>
     * This is equivalent to {@link #put(TransactionHandle, String, RecordBuffer, PutType)
     * put(transaction, table, record, PutType.DEFAULT_BEHAVIOR)}.
     * </p>
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param transaction the context transaction handle
     * @param table the target table
     * @param record the row data to put to the target table
     * @return a future response of this action.
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     * @see PutType#DEFAULT_BEHAVIOR
     * @see #put(TransactionHandle, String, RecordBuffer, PutType)
     */
    default FutureResponse<PutResult> put(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer record) throws IOException {
        return put(transaction, table, record, PutType.DEFAULT_BEHAVIOR);
    }

    /**
     * Requests {@code PUT} operation, which creates or updates a row on the table.
     * <p>
     * This does not raise errors even if the operation does not actually update the table by restriction of
     * {@link PutType} behavior.
     * You can check it use {@link PutResult#size()}.
     * </p>
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param transaction the context transaction handle
     * @param table the target table
     * @param record the row data to put to the target table
     * @param behavior the operation behavior
     * @return a future response of this action.
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<PutResult> put(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer record, @Nonnull PutType behavior) throws IOException;

    /**
     * Requests {@code REMOVE} operation, which deletes a row on the table.
     * <p>
     * This is equivalent to {@link #remove(TransactionHandle, String, RecordBuffer, RemoveType)
     * put(transaction, table, key, RemoveType.DEFAULT_BEHAVIOR)}.
     * </p>
     * <p>
     * This does not raise errors even if there is no such the record for the key.
     * You can check it use {@link RemoveResult#size()}.
     * </p>
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param transaction the context transaction handle
     * @param table the target table
     * @param key the index key of the target table
     * @return a future response of this action.
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     * @see #remove(TransactionHandle, String, RecordBuffer, RemoveType)
     * @see RemoveType#DEFAULT_BEHAVIOR
     */
    default FutureResponse<RemoveResult> remove(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer key) throws IOException {
        return remove(transaction, table, key, RemoveType.DEFAULT_BEHAVIOR);
    }

    /**
     * Requests {@code REMOVE} operation, which deletes a row on the table.
     * <p>
     * This does not raise errors even if there is no such the record for the key.
     * You can check it use {@link RemoveResult#size()} only if you set {@link RemoveType#COUNTING}.
     * </p>
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param transaction the context transaction handle
     * @param table the target table
     * @param key the index key of the target table
     * @param behavior the operation behavior
     * @return a future response of this action.
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<RemoveResult> remove(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer key, @Nonnull RemoveType behavior) throws IOException;

    /**
     * Requests {@code SCAN} operation, which collects all records contained between two keys on the table.
     * <p>
     * This is equivalent to
     * {@link #scan(TransactionHandle, String, RecordBuffer, ScanBound, RecordBuffer, ScanBound, ScanType)
     * scan(transaction, table, lowerKey, lowerBound, upperKey, upperBound, ScanType.DEFAULT_BEHAVIOR)}.
     * </p>
     * <p>
     * You can pass {@code null} to the lower or upper key to perform without scan range on the table.
     * To perform table full-scan, you must pass {@code null} to both keys.
     * </p>
     * <p>
     * This operation will return a {@link RecordCursor} to retrieve the scan results.
     * You must close it cursor after obtains the results.
     * </p>
     * <p>
     * This will take copies of {@link RecordBuffer}.
     * </p>
     * @param transaction the context transaction handle
     * @param table the source table
     * @param lowerKey the lower index key (or its prefix) of scan range on the target table
     * @param lowerBound whether includes or excludes the record on the lower key
     * @param upperKey the upper index key (or its prefix) of scan range on the target table
     * @param upperBound whether includes or excludes the record on the upper key
     * @return a future response of the scan cursor
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     * @see #scan(TransactionHandle, String, RecordBuffer, ScanBound, RecordBuffer, ScanBound, ScanType)
     * @see ScanType#DEFAULT_BEHAVIOR
     */
    default FutureResponse<RecordCursor> scan(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table,
            @Nullable RecordBuffer lowerKey, @Nullable ScanBound lowerBound,
            @Nullable RecordBuffer upperKey, @Nullable ScanBound upperBound) throws IOException {
        return scan(transaction, table, lowerKey, lowerBound, upperKey, upperBound, ScanType.DEFAULT_BEHAVIOR);
    }

    /**
     * Requests {@code SCAN} operation, which collects all records contained between two keys on the table.
     * <p>
     * You can pass {@code null} to the lower or upper key to perform without scan range on the table.
     * To perform table full-scan, you must pass {@code null} to both keys.
     * </p>
     * <p>
     * This operation will return a {@link RecordCursor} to retrieve the scan results.
     * You must close it cursor after obtains the results.
     * </p>
     * <p>
     * This will take copies of {@link RecordBuffer}.
     * </p>
     * @param transaction the context transaction handle
     * @param table the source table
     * @param lowerKey the lower index key (or its prefix) of scan range on the target table
     * @param lowerBound whether includes or excludes the record on the lower key
     * @param upperKey the upper index key (or its prefix) of scan range on the target table
     * @param upperBound whether includes or excludes the record on the upper key
     * @param behavior the operation behavior
     * @return a future response of the scan cursor
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<RecordCursor> scan(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table,
            @Nullable RecordBuffer lowerKey, @Nullable ScanBound lowerBound,
            @Nullable RecordBuffer upperKey, @Nullable ScanBound upperBound,
            @Nonnull ScanType behavior) throws IOException;

    /**
     * Requests the sequence of KVS operations in the given script.
     * <p>
     * This is equivalent to {@link #batch(TransactionHandle, BatchScript) batch(null, script)}.
     * </p>
     * @param script the script of operations
     * @return a future response of the operation
     * @throws IllegalArgumentException the operation does not start a new transaction
     * @throws IOException if I/O error was occurred while sending request
     */
    default FutureResponse<BatchResult> batch(@Nonnull BatchScript script) throws IOException {
        return batch(null, script);
    }

    /**
     * Requests the sequence of KVS operations in the given script.
     * <p>
     * If the operation will start a {@link BatchScript#newTransaction() new transaction} at the beginning,
     * you must pass {@code null} to the context transaction (the first argument).
     * Otherwise, you must pass the existing transaction handle.
     * </p>
     * <p>
     * The batch operation succeeds only if all operations included in the script were succeeded,
     * or raise a typical error in the included operations.
     * </p>
     * <p>
     * This may raise an error if there are too many operations in the script, or returns too many records.
     * In such the case, you should split
     * </p>
     * @param transaction the context transaction handle,
     *      or {@code null} if the operation will start a new transaction at the beginning
     * @param script the script of operations
     * @return a future response of the operation
     * @throws IllegalArgumentException if {@code transaction} is set
     *      and the operation will start a new transaction
     * @throws IllegalArgumentException if {@code transaction} is {@code null}
     *      and the operation does not start a new transaction
     * @throws IllegalArgumentException if the transaction handle is not supported
     * @throws IOException if I/O error was occurred while sending request
     */
    FutureResponse<BatchResult> batch(@Nullable TransactionHandle transaction, @Nonnull BatchScript script) throws IOException;

    /**
     * Disposes the underlying server resources.
     * Note that, this never closes the underlying {@link Session}.
     */
    @Override
    default void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
