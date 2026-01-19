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
package com.tsurugidb.tsubakuro.datastore.impl;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.datastore.proto.DatastoreRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.datastore.Backup;
import com.tsurugidb.tsubakuro.datastore.BackupDetail;
import com.tsurugidb.tsubakuro.datastore.BackupEstimate;
import com.tsurugidb.tsubakuro.datastore.BackupType;
import com.tsurugidb.tsubakuro.datastore.DatastoreClient;
import com.tsurugidb.tsubakuro.datastore.DatastoreService;
import com.tsurugidb.tsubakuro.datastore.Tag;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link DatastoreClient}.
 */
@ThreadSafe
public class DatastoreClientImpl implements DatastoreClient {

    private final DatastoreService service;

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    public static DatastoreClientImpl attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        return new DatastoreClientImpl(new DatastoreServiceStub(session));
    }

    /**
     * Creates a new instance.
     * @param service the service stub
     */
    public DatastoreClientImpl(@Nonnull DatastoreService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public FutureResponse<Backup> beginBackup(@Nullable String label) throws IOException {
        var builder = DatastoreRequest.BackupBegin.newBuilder();
        if (label != null) {
            builder.setLabel(label);
        }
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<BackupDetail> beginBackup(@Nonnull BackupType type) throws IOException {
        return beginBackup(type, null);
    }

    @Override
    public FutureResponse<BackupDetail> beginBackup(@Nonnull BackupType type, @Nullable String label) throws IOException {
        var builder = DatastoreRequest.BackupDetailBegin.newBuilder();
        if (label != null) {
            builder.setLabel(label)
            .setType(type.type());
        }
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<BackupEstimate> estimateBackup() throws IOException {
        var builder = DatastoreRequest.BackupEstimate.newBuilder();
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<List<Tag>> listTag() throws IOException {
        var builder = DatastoreRequest.TagList.newBuilder();
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Optional<Tag>> getTag(@Nonnull String name) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequest.TagGet.newBuilder()
                .setName(name);
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Tag> addTag(@Nonnull String name, @Nullable String comment) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequest.TagAdd.newBuilder()
                .setName(name);
        if (comment != null) {
            builder.setComment(comment);
        }
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Boolean> removeTag(@Nonnull String name) throws IOException {
        Objects.requireNonNull(name);
        var builder = DatastoreRequest.TagRemove.newBuilder()
                .setName(name);
        return service.send(builder.build());
    }

    @Override
    public FutureResponse<Void> registerTransactionTpmId(@Nonnull String transactionID, Long tpmID) throws IOException {
        Objects.requireNonNull(transactionID);
        var builder = DatastoreRequest.RegisterTransactionTpmId.newBuilder()
                .setTransactionId(transactionID)
                .setTpmId(tpmID);
        return service.send(builder.build());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        service.close();
    }
}
