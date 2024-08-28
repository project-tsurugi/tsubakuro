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
package com.tsurugidb.tsubakuro.kvs.impl;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.BatchResult;
import com.tsurugidb.tsubakuro.kvs.BatchScript;
import com.tsurugidb.tsubakuro.kvs.CommitType;
import com.tsurugidb.tsubakuro.kvs.GetResult;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutResult;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.RecordCursor;
import com.tsurugidb.tsubakuro.kvs.RemoveResult;
import com.tsurugidb.tsubakuro.kvs.RemoveType;
import com.tsurugidb.tsubakuro.kvs.ScanBound;
import com.tsurugidb.tsubakuro.kvs.ScanType;
import com.tsurugidb.tsubakuro.kvs.TransactionHandle;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link KvsClient}.
 */
public class KvsClientImpl implements KvsClient {

    private final KvsService service;

    /**
     * Attaches to the SQL service in the current session.
     * @param session the current session
     * @return the SQL service client
     */
    public static KvsClientImpl attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        var service = new KvsServiceStub(session);
        session.put(service);
        return new KvsClientImpl(service);
    }

    /**
     * Creates a new instance.
     * @param service the target service.
     */
    public KvsClientImpl(@Nonnull KvsService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public FutureResponse<TransactionHandle> beginTransaction(@Nonnull TransactionOption option) throws IOException {
        Objects.requireNonNull(option);
        var builder = KvsRequest.Begin.newBuilder()
                .setTransactionOption(option.getEntity());
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Void> commit(
            @Nonnull TransactionHandle transaction, @Nonnull CommitType behavior) throws IOException {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(behavior);
        var handle = service.extract(transaction);
        var builder = KvsRequest.Commit.newBuilder()
                .setTransactionHandle(handle)
                .setNotificationType(BatchScript.convert(behavior))
                .setAutoDispose(true);
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Void> rollback(@Nonnull TransactionHandle transaction) throws IOException {
        Objects.requireNonNull(transaction);
        var handle = service.extract(transaction);
        var builder = KvsRequest.Rollback.newBuilder()
                .setTransactionHandle(handle);
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<GetResult> get(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer key) throws IOException {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(table);
        Objects.requireNonNull(key);
        var handle = service.extract(transaction);
        var builder = KvsRequest.Get.newBuilder()
                .setTransactionHandle(handle)
                .setIndex(KvsRequest.Index.newBuilder()
                        .setTableName(table))
                .addKeys(key.toRecord().getEntity());
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<PutResult> put(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer record, @Nonnull PutType behavior) throws IOException {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(table);
        Objects.requireNonNull(record);
        Objects.requireNonNull(behavior);
        var handle = service.extract(transaction);
        var builder = KvsRequest.Put.newBuilder()
                .setTransactionHandle(handle)
                .setIndex(KvsRequest.Index.newBuilder()
                        .setTableName(table))
                .addRecords(record.toRecord().getEntity())
                .setType(convert(behavior));
        return service.send(builder.build());
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

    @Override
    public FutureResponse<RemoveResult> remove(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table, @Nonnull RecordBuffer key, @Nonnull RemoveType behavior) throws IOException {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(table);
        Objects.requireNonNull(key);
        Objects.requireNonNull(behavior);
        var handle = service.extract(transaction);
        var builder = KvsRequest.Remove.newBuilder()
                .setTransactionHandle(handle)
                .setIndex(KvsRequest.Index.newBuilder()
                        .setTableName(table))
                .addKeys(key.toRecord().getEntity())
                .setType(convert(behavior));
        return service.send(builder.build());
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

    @Override
    public FutureResponse<RecordCursor> scan(
            @Nonnull TransactionHandle transaction,
            @Nonnull String table,
            @Nullable RecordBuffer lowerKey, @Nullable ScanBound lowerBound,
            @Nullable RecordBuffer upperKey, @Nullable ScanBound upperBound,
            @Nonnull ScanType behavior) throws IOException {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(table);
        Objects.requireNonNull(behavior);
        var handle = service.extract(transaction);
        var builder = KvsRequest.Scan.newBuilder()
                .setTransactionHandle(handle)
                .setIndex(KvsRequest.Index.newBuilder()
                        .setTableName(table));
        if (lowerKey == null) {
            builder.setLowerBound(KvsRequest.Scan.Bound.SCAN_BOUND_UNSPECIFIED);
        } else {
            Objects.requireNonNull(lowerBound);
            builder.setLowerKey(lowerKey.toRecord().getEntity());
            builder.setLowerBound(convert(lowerBound));
        }
        if (upperKey == null) {
            builder.setUpperBound(KvsRequest.Scan.Bound.SCAN_BOUND_UNSPECIFIED);
        } else {
            Objects.requireNonNull(upperBound);
            builder.setUpperKey(upperKey.toRecord().getEntity());
            builder.setUpperBound(convert(upperBound));
        }
        builder.setType(convert(behavior));
        builder.setChannelName(Constants.SCAN_CHANNEL_NAME);
        return service.send(builder.build());
    }

    private static KvsRequest.Scan.Bound convert(ScanBound bound) {
        assert bound != null;
        switch (bound) {
        case INCLUSIVE:
            return KvsRequest.Scan.Bound.INCLUSIVE;
        case EXCLUSIVE:
            return KvsRequest.Scan.Bound.EXCLUSIVE;
        }
        throw new IllegalArgumentException(String.valueOf(bound));
    }

    private static KvsRequest.Scan.Type convert(ScanType behavior) {
        assert behavior != null;
        switch (behavior) {
        case FORWARD:
            return KvsRequest.Scan.Type.FORWARD;
        case BACKWARD:
            return KvsRequest.Scan.Type.BACKWARD;
        }
        throw new IllegalArgumentException(String.valueOf(behavior));
    }

    @Override
    public FutureResponse<BatchResult> batch(
            @Nullable TransactionHandle transaction,
            @Nonnull BatchScript script) throws IOException {
        Objects.requireNonNull(script);
        var handle = Optional.ofNullable(transaction)
                .map(service::extract)
                .orElse(null);
        var request = script.build(handle);
        return service.send(request);
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
        if (service != null) {
            service.close();
        }
    }
}
