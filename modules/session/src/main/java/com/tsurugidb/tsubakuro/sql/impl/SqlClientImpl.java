package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.Transaction;
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
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
        if (Objects.nonNull(service)) {
            service.close();
        }
    }
}
