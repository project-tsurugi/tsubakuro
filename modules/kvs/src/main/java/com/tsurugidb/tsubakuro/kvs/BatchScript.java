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

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsTransaction;

/**
 * Represents a script to organize a series of KVS operations as batch.
 */
public class BatchScript {

    /**
     * Represents an operation type of batch element.
     */
    public enum OperationType {

        /**
         * {@code GET} operation.
         * @see KvsClient#get(TransactionHandle, String, RecordBuffer)
         * @see GetResult
         */
        GET(GetResult.class),

        /**
         * {@code PUT} operation.
         * @see KvsClient#put(TransactionHandle, String, RecordBuffer)
         * @see PutResult
         */
        PUT(PutResult.class),

        /**
         * {@code REMOVE} operation.
         * @see KvsClient#remove(TransactionHandle, String, RecordBuffer)
         * @see RemoveResult
         */
        REMOVE(RemoveResult.class),
        ;

        private static final Map<Class<?>, OperationType> REVERSE = Arrays.asList(values()).stream()
                .collect(Collectors.toMap(OperationType::getResultType, Function.identity()));

        private final Class<?> resultType;

        OperationType(Class<?> resultType) {
            Objects.requireNonNull(resultType);
            this.resultType = resultType;
        }

        /**
         * Returns the corresponding result type.
         * @return the corresponding result type
         */
        public Class<?> getResultType() {
            return resultType;
        }

        /**
         * Returns the operation type from its result type.
         * @param aClass the result type class
         * @return the corresponding operation type
         * @throws IllegalArgumentException if input is unsupported operation type
         */
        public static OperationType fromResultType(@Nonnull Class<?> aClass) {
            Objects.requireNonNull(aClass);
            var result = REVERSE.get(aClass);
            if (result == null) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "unsupported result type: {0}",
                        aClass.getName()));
            }
            return result;
        }
    }

    /**
     * Represents a reference to the individual operation results.
     * @param <T> the element result type
     */
    public static class Ref<T> {

        private final int index;

        private final Class<T> resultType;

        private final OperationType operationType;

        /**
         * Creates a new instance.
         * @param index the operation index in the script (0-origin)
         * @param resultType the operation result type
         */
        public Ref(@Nonnegative int index, @Nonnull Class<T> resultType) {
            if (index < 0) {
                throw new IndexOutOfBoundsException();
            }
            Objects.requireNonNull(resultType);

            this.index = index;
            this.resultType = resultType;
            this.operationType = OperationType.fromResultType(resultType);
        }

        /**
         * Returns the operation index in the script.
         * @return the operation index (0-origin)
         */
        public int getIndex() {
            return index;
        }

        /**
         * Returns the operation result type.
         * @return the operation result type
         */
        public Class<T> getResultType() {
            return resultType;
        }

        /**
         * Returns the operation type.
         * @return the operation type
         */
        public OperationType getOperationType() {
            return operationType;
        }

        @Override
        public String toString() {
            return String.format("%s[%d]", getOperationType(), getIndex()); //$NON-NLS-1$
        }
    }

    private final KvsRequest.Batch.Builder builder = KvsRequest.Batch.newBuilder();

    /**
     * Builds a request object form this script.
     * @param transaction the context transaction, or {@code null} to use the intrinsic transaction
     * @return the request object
     */
    public KvsRequest.Batch build(@Nullable KvsTransaction.Handle transaction) {
        var beginTx = builder.getTransactionCase() == KvsRequest.Batch.TransactionCase.BEGIN;

        // no transactions
        if (transaction == null && !beginTx) {
            throw new IllegalArgumentException("transaction is not set");
        }

        // conflict transactions
        if (transaction != null && beginTx) {
            throw new IllegalArgumentException("transaction is already reserved in the batch");
        }

        // use external transaction
        if (transaction != null) {
            builder.setTransactionHandle(transaction);
        }
        return builder.build();
    }

    /**
     * Starts a new transaction at the beginning of this batch operation, instead of using an existing transaction.
     * Note that, the started transaction will automatically committed at the end of this batch, even if
     * {@link #addCommit()} is not specified (you can also direct {@link #addCommit()} explicitly).
     * @see KvsClient#beginTransaction()
     */
    public void newTransaction() {
        newTransaction(new TransactionOption());
    }

    /**
     * Starts a new transaction at the beginning of this batch operation, instead of using an existing transaction.
     * Note that, the started transaction will automatically committed at the end of this batch, even if
     * {@link #addCommit()} is not specified (you can also direct {@link #addCommit()} explicitly).
     * <p>
     * If you invoke this operation twice or more, only the last invocation will be available.
     * </p>
     * @param option the transaction option
     * @throws IllegalStateException if a new transaction is already reserved in this script
     * @see KvsClient#beginTransaction()
     */
    public void newTransaction(@Nonnull TransactionOption option) {
        Objects.requireNonNull(option);
        builder.setBegin(KvsRequest.Begin.newBuilder()
                .setTransactionOption(option.getEntity()));
        // set implicit commit operation
        if (builder.getCommitOptionalCase() == KvsRequest.Batch.CommitOptionalCase.COMMITOPTIONAL_NOT_SET) {
            addCommit();
        }
    }

    /**
     * Commits the transaction at the end of this batch operation.
     * <p>
     * This is equivalent to {@link #addCommit(CommitType) addCommit(CommitType#DEFAULT_BEHAVIOR)}.
     * </p>
     * <p>
     * If you invoke this operation twice or more, only the last invocation will be available.
     * </p>
     * @see #addCommit(CommitType)
     * @see CommitType#DEFAULT_BEHAVIOR
     * @see KvsClient#commit(TransactionHandle)
     */
    public void addCommit() {
        addCommit(CommitType.DEFAULT_BEHAVIOR);
    }

    /**
     * Commits the transaction at the end of this batch operation.
     * <p>
     * If you invoke this operation twice or more, only the last invocation will be available.
     * </p>
     * @param behavior the operation behavior
     * @see KvsClient#commit(TransactionHandle, CommitType)
     */
    public void addCommit(@Nonnull CommitType behavior) {
        Objects.requireNonNull(behavior);
        builder.setCommit(KvsRequest.Commit.newBuilder()
                .setNotificationType(convert(behavior))
                .setAutoDispose(true));
    }

    /**
     * Retrieves CommitStatus corresponds to the CommitType
     * @param behavior the operation behavior
     * @return the CommitStatus value corresponds to the behavior
     */
    public static KvsRequest.CommitStatus convert(CommitType behavior) {
        assert behavior != null;
        switch (behavior) {
        case UNSPECIFIED:
            return KvsRequest.CommitStatus.COMMIT_STATUS_UNSPECIFIED;
        case ACCEPTED:
            return KvsRequest.CommitStatus.ACCEPTED;
        case AVAILABLE:
            return KvsRequest.CommitStatus.AVAILABLE;
        case STORED:
            return KvsRequest.CommitStatus.STORED;
        case PROPAGATED:
            return KvsRequest.CommitStatus.PROPAGATED;
        }
        throw new IllegalArgumentException(String.valueOf(behavior));
    }

    /**
     * Adds {@code GET} operation to this batch.
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param table the source table name
     * @param key the target key
     * @return a reference to retrieve the result of this operation
     * @see KvsClient#get(TransactionHandle, String, RecordBuffer)
     * @see BatchResult#get(Ref)
     */
    public Ref<GetResult> addGet(@Nonnull String table, @Nonnull RecordBuffer key) {
        Objects.requireNonNull(table);
        Objects.requireNonNull(key);
        int index = builder.getElementsCount();
        builder.addElements(KvsRequest.Batch.ScriptElement.newBuilder()
                .setGet(KvsRequest.Get.newBuilder()
                        .setIndex(KvsRequest.Index.newBuilder()
                                .setTableName(table))
                        .addKeys(key.toRecord().getEntity())));
        return new Ref<>(index, GetResult.class);
    }

    /**
     * Adds {@code PUT} operation to this batch.
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param table the destination table name
     * @param record the record to put
     * @return a reference to retrieve the result of this operation
     * @see KvsClient#put(TransactionHandle, String, RecordBuffer)
     * @see BatchResult#get(Ref)
     */
    public Ref<PutResult> addPut(@Nonnull String table, @Nonnull RecordBuffer record) {
        return addPut(table, record, PutType.DEFAULT_BEHAVIOR);
    }

    private static KvsRequest.Put.Type convert(PutType behavior) {
        assert behavior != null;
        switch (behavior) {
        case OVERWRITE:
            return KvsRequest.Put.Type.OVERWRITE;
        case IF_ABSENT:
            return KvsRequest.Put.Type.IF_ABSENT;
        case IF_PRESENT:
            return KvsRequest.Put.Type.IF_PRESENT;
        }
        throw new IllegalArgumentException(String.valueOf(behavior));
    }

    /**
     * Adds {@code PUT} operation to this batch.
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param table the destination table name
     * @param record the record to put
     * @param behavior the behavior option
     * @return a reference to retrieve the result of this operation
     * @see KvsClient#put(TransactionHandle, String, RecordBuffer, PutType)
     * @see BatchResult#get(Ref)
     */
    public Ref<PutResult> addPut(@Nonnull String table, @Nonnull RecordBuffer record, @Nonnull PutType behavior) {
        Objects.requireNonNull(table);
        Objects.requireNonNull(record);
        Objects.requireNonNull(behavior);
        int index = builder.getElementsCount();
        builder.addElements(KvsRequest.Batch.ScriptElement.newBuilder()
                .setPut(KvsRequest.Put.newBuilder()
                        .setIndex(KvsRequest.Index.newBuilder()
                                .setTableName(table))
                        .addRecords(record.toRecord().getEntity())
                        .setType(convert(behavior))));
        return new Ref<>(index, PutResult.class);
    }

    /**
     * Adds {@code REMOVE} operation to this batch.
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param table the destination table name
     * @param key the record key to remove
     * @return a reference to retrieve the result of this operation
     * @see KvsClient#remove(TransactionHandle, String, RecordBuffer)
     * @see BatchResult#get(Ref)
     */
    public Ref<RemoveResult> addRemove(@Nonnull String table, @Nonnull RecordBuffer key) {
        return addRemove(table, key, RemoveType.DEFAULT_BEHAVIOR);
    }

    /**
     * Adds {@code REMOVE} operation to this batch.
     * <p>
     * This will take a copy of {@link RecordBuffer}.
     * </p>
     * @param table the destination table name
     * @param key the record key to remove
     * @param behavior the behavior option
     * @return a reference to retrieve the result of this operation
     * @see KvsClient#remove(TransactionHandle, String, RecordBuffer, RemoveType)
     * @see BatchResult#get(Ref)
     */
    public Ref<RemoveResult> addRemove(@Nonnull String table, @Nonnull RecordBuffer key, @Nonnull RemoveType behavior) {
        Objects.requireNonNull(table);
        Objects.requireNonNull(key);
        Objects.requireNonNull(behavior);
        int index = builder.getElementsCount();
        builder.addElements(KvsRequest.Batch.ScriptElement.newBuilder()
                .setRemove(KvsRequest.Remove.newBuilder()
                        .setIndex(KvsRequest.Index.newBuilder()
                                .setTableName(table))
                        .addKeys(key.toRecord().getEntity())
                        .setType(convert(behavior))));
        return new Ref<>(index, RemoveResult.class);
    }

    private static KvsRequest.Remove.Type convert(RemoveType behavior) {
        assert behavior != null;
        switch (behavior) {
        case COUNTING:
            return KvsRequest.Remove.Type.COUNTING;
        case INSTANT:
            return KvsRequest.Remove.Type.INSTANT;
        }
        throw new IllegalArgumentException(String.valueOf(behavior));
    }
}
