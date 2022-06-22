package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.TableMetadata;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link SqlClient}.
 */
public class SqlClientImpl implements SqlClient {

    private final Session session;
    private SessionLinkImpl sessionLinkImpl;

    /**
     * Creates a new instance.
     * @param session the attached session
     */
    public SqlClientImpl(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
        sessionLinkImpl = new SessionLinkImpl(session.getWire());
    }

    /**
     * Begin a new transaction
     * @param readOnly specify whether the new transaction is read-only or not
     * @return the transaction
     */
//    @Override
    @Deprecated
    public FutureResponse<Transaction> createTransaction(boolean readOnly) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("this session is not connected to the Database");
        }
        return new FutureTransactionImpl(sessionLinkImpl.send(
                                             SqlRequest.Begin.newBuilder()
                                                 .setOption(SqlRequest.TransactionOption.newBuilder()
                                                                .setType(readOnly ? SqlRequest.TransactionType.READ_ONLY : SqlRequest.TransactionType.SHORT)
                                                            )
                                             ),
                                         sessionLinkImpl);
    }

    /**
     * Begin a new read-write transaction
     * @return the transaction
     */
    @Override
    public FutureResponse<Transaction> createTransaction() throws IOException {
        return createTransaction(SqlRequest.TransactionOption.newBuilder().build());
    }

    /**
     * Begin a new transaction by specifying the transaction type
     * @return the transaction
     */
    @Override
    public FutureResponse<Transaction> createTransaction(SqlRequest.TransactionOption option) throws IOException {
        if (Objects.isNull(sessionLinkImpl)) {
            throw new IOException("this session is not connected to the Database");
        }
        return new FutureTransactionImpl(sessionLinkImpl.send(SqlRequest.Begin.newBuilder().setOption(option)), sessionLinkImpl);
    }

    @Override
    public FutureResponse<PreparedStatement> prepare(
            @Nonnull String source,
            @Nonnull Collection<? extends SqlRequest.PlaceHolder> placeholders) throws IOException {
        Objects.requireNonNull(source);
        Objects.requireNonNull(placeholders);
        var pb = SqlRequest.Prepare.newBuilder().setSql(source);
        for (SqlRequest.PlaceHolder e : placeholders) {
            pb.addPlaceholders(e);
        }
        return sessionLinkImpl.send(pb);
    }

    @Override
    public FutureResponse<String> explain(
            @Nonnull PreparedStatement statement,
            @Nonnull Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
        Objects.requireNonNull(statement);
        Objects.requireNonNull(parameters);
        var pb = SqlRequest.Explain.newBuilder().setPreparedStatementHandle(((PreparedStatementImpl) statement).getHandle());
        for (SqlRequest.Parameter e : parameters) {
            pb.addParameters(e);
        }
        return sessionLinkImpl.send(pb);
    }

    @Override
    public FutureResponse<TableMetadata> getTableMetadata(@Nonnull String tableName) throws IOException {
        Objects.requireNonNull(tableName);
        var resuest = SqlRequest.DescribeTable.newBuilder()
                .setName(tableName)
                .build();
        return sessionLinkImpl.send(resuest);
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // FIXME close underlying resources (e.g. ongoing transactions)
        if (Objects.nonNull(sessionLinkImpl)) {
            sessionLinkImpl.close();
            sessionLinkImpl = null;
        }
    }
}
