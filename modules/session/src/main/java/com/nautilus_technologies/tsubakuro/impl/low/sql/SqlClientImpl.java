package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

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
    public FutureResponse<Transaction> createTransaction(@Nonnull RequestProtos.TransactionOption option)
            throws IOException {
        Objects.requireNonNull(option);
        return session.createTransaction(option);
    }

    @Override
    public FutureResponse<PreparedStatement> prepare(
            @Nonnull String source,
            @Nonnull Collection<? extends RequestProtos.PlaceHolder.Variable> placeholders) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(placeholders);
        return session.prepare(source, RequestProtos.PlaceHolder.newBuilder()
                .addAllVariables(placeholders)
                .build());
    }

    @Override
    public FutureResponse<String> explain(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends RequestProtos.ParameterSet.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        return session.explain(statement, RequestProtos.ParameterSet.newBuilder()
                .addAllParameters(parameters)
                .build());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
    }
}
