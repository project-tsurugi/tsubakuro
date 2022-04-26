package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;

/**
 * An implementation of {@link SqlClient}.
 */
public class SqlClientImpl implements SqlClient {

    private final Session session;

    /**
     * Creates a new instance.
     * @param session the attached session
     */
    public SqlClientImpl(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    // FIXME directly send request messages instead of delegate it via Session

    @Override
    public Future<Transaction> createTransaction(@Nonnull RequestProtos.TransactionOption option)
            throws IOException {
        Objects.requireNonNull(option);
        return session.createTransaction(option);
    }

    @Override
    public Future<PreparedStatement> prepare(@Nonnull String sql, @Nonnull RequestProtos.PlaceHolder placeHolder)
            throws IOException {
        Objects.requireNonNull(sql);
        Objects.requireNonNull(placeHolder);
        return session.prepare(sql, placeHolder);
    }

    @Override
    public Future<String> explain(@Nonnull PreparedStatement statement, @Nonnull RequestProtos.ParameterSet parameters)
            throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        return session.explain(statement, parameters);
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
    }
}
