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
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SearchPath;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.LargeObjectCache;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link SqlClient}.
 */
public class SqlClientImpl implements SqlClient {

    private final SqlService service;

    /**
     * Attaches to the datastore service in the current session.
     * @param session the current session
     * @return the datastore service client
     */
    public static SqlClientImpl attach(@Nonnull Session session) {
        Objects.requireNonNull(session);
        var service = new SqlServiceStub(session);
        session.put(service);
        return new SqlClientImpl(service);
    }

    /**
     * Creates a new instance.
     * @param service the service stub
     */
    public SqlClientImpl(@Nonnull SqlService service) {
        Objects.requireNonNull(service);
        this.service = service;
    }

    @Override
    public FutureResponse<Transaction> createTransaction(
            @Nonnull SqlRequest.TransactionOption option) throws IOException {
        Objects.requireNonNull(option);
        var request = SqlRequest.Begin.newBuilder()
                .setOption(option)
                .build();
        return service.send(request);
    }

    @Override
    public FutureResponse<PreparedStatement> prepare(
            @Nonnull String source,
            @Nonnull Collection<? extends SqlRequest.Placeholder> placeholders) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(placeholders);
        var resuest = SqlRequest.Prepare.newBuilder()
                .setSql(source)
                .addAllPlaceholders(placeholders)
                .build();
        return service.send(resuest);
    }

    @Override
    public FutureResponse<StatementMetadata> explain(@Nonnull String source) throws IOException {
        var resuest = SqlRequest.ExplainByText.newBuilder()
                .setSql(source)
                .build();
        return service.send(resuest);
    }

    @Override
    public FutureResponse<StatementMetadata> explain(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        var resuest = SqlRequest.Explain.newBuilder()
                .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
                .addAllParameters(parameters)
                .build();
        return service.send(resuest);
    }

    @Override
    public FutureResponse<TableMetadata> getTableMetadata(@Nonnull String tableName) throws IOException {
        Objects.requireNonNull(tableName);
        var resuest = SqlRequest.DescribeTable.newBuilder()
                .setName(tableName)
                .build();
        return service.send(resuest);
    }

    @Override
    public FutureResponse<ExecuteResult> executeLoad(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters,
            @Nonnull Collection<? extends Path> files) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        Objects.requireNonNull(files);
        var pb = SqlRequest.ExecuteLoad.newBuilder()
        .setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle())
        .addAllFile(files.stream()
        .map(Path::toString)
        .collect(Collectors.toList()));
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<TableList> listTables() throws IOException {
        var pb = SqlRequest.ListTables.newBuilder();
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<SearchPath> getSearchPath() throws IOException {
        var pb = SqlRequest.GetSearchPath.newBuilder();
        return service.send(pb.build());
    }

    @Override
    public FutureResponse<InputStream> openInputStream(@Nonnull BlobReference ref) throws IOException {
        Objects.requireNonNull(ref);
        if (ref instanceof BlobReferenceForSql) {
            var blobReferenceForSql = (BlobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setReference(blobReferenceForSql.blobReference());
            return service.send(pb.build(), ref);
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<Reader> openReader(@Nonnull ClobReference ref) throws IOException {
        Objects.requireNonNull(ref);
        if (ref instanceof ClobReferenceForSql) {
            var clobReferenceForSql = (ClobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setReference(clobReferenceForSql.clobReference());
            return service.send(pb.build(), ref);
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<LargeObjectCache> getLargeObjectCache(@Nonnull BlobReference ref) throws IOException {
        Objects.requireNonNull(ref);
        if (ref instanceof BlobReferenceForSql) {
            var blobReferenceForSql = (BlobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setReference(blobReferenceForSql.blobReference());
            return service.send(pb.build());
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<LargeObjectCache> getLargeObjectCache(@Nonnull ClobReference ref) throws IOException {
        Objects.requireNonNull(ref);
        if (ref instanceof ClobReferenceForSql) {
            var clobReferenceForSql = (ClobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setReference(clobReferenceForSql.clobReference());
            return service.send(pb.build());
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<Void> copyTo(@Nonnull BlobReference ref, @Nonnull Path destination) throws IOException {
        Objects.requireNonNull(ref);
        if (ref instanceof BlobReferenceForSql) {
            var refForSql = (BlobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setReference(refForSql.blobReference());
            return service.send(pb.build(), destination);
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public FutureResponse<Void> copyTo(@Nonnull ClobReference ref, @Nonnull Path destination) throws IOException {
        Objects.requireNonNull(ref);
        Objects.requireNonNull(destination);
        if (ref instanceof ClobReferenceForSql) {
            var refForSql = (ClobReferenceForSql) ref;
            var pb = SqlRequest.GetLargeObjectData.newBuilder()
                        .setReference(refForSql.clobReference());
            return service.send(pb.build(), destination);
        }
        throw new IllegalStateException(ref.getClass().getName() + "is unsupported.");
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
        if (service != null) {
            service.close();
        }
    }
}
