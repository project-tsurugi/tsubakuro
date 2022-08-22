package com.tsurugidb.tsubakuro.console.executor;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.tsurugidb.tateyama.proto.SqlRequest;
import com.tsurugidb.tsubakuro.console.model.Region;

/**
 * A basic implementation of {@link SqlProcessor}.
 */
public class BasicSqlProcessor implements SqlProcessor {

    static final Logger LOG = LoggerFactory.getLogger(BasicSqlProcessor.class);

    private final SqlClient client;

    private Transaction transaction;

    /**
     * Creates a new instance.
     * @param client the SQL client: It will be closed after this object was closed
     */
    public BasicSqlProcessor(@Nonnull SqlClient client) {
        Objects.requireNonNull(client);
        this.client = client;
    }

    @Override
    public void startTransaction(
            @Nonnull SqlRequest.TransactionOption option) throws ServerException, IOException, InterruptedException {
        Objects.requireNonNull(option);
        desireInactive();
        LOG.debug("start transaction: {}", option);
        transaction = client.createTransaction(option).await();
    }

    @Override
    public void commitTransaction(
            @Nullable SqlRequest.CommitStatus status) throws ServerException, IOException, InterruptedException {
        LOG.debug("start commit: {}", status); //$NON-NLS-1$
        desireActive();
        try (var t = transaction) {
            transaction = null;
            if (status == null) {
                t.commit().await();
            } else {
                t.commit(status).await();
            }
        }
    }

    @Override
    public void rollbackTransaction() throws ServerException, IOException, InterruptedException {
        LOG.debug("start rollback"); //$NON-NLS-1$
        if (isTransactionActive()) {
            try (var t = transaction) {
                transaction = null;
                t.rollback().await();
            }
        } else {
            LOG.warn("rollback request is ignored because transaction is not active");
        }
    }

    @Override
    public @Nullable ResultSet execute(
            @Nonnull String statement,
            @Nullable Region region) throws ServerException, IOException, InterruptedException {
        Objects.requireNonNull(statement);
        LOG.debug("start prepare: '{}'", statement);
        desireActive();
        try (var prepared = client.prepare(statement).await()) {
            if (prepared.hasResultRecords()) {
                LOG.debug("start query: '{}'", statement);
                return transaction.executeQuery(prepared).await();
            }
            LOG.debug("start execute: '{}'", statement);
            transaction.executeStatement(prepared).await();
            return null;
        }
    }

    @Override
    public boolean isTransactionActive() {
        return transaction != null;
    }

    /**
     * Returns the running transaction.
     * @return the running transaction, or {@code null} if there is no active transactions
     */
    public @Nullable Transaction getTransaction() {
        return transaction;
    }

    private void desireActive() {
        if (!isTransactionActive()) {
            throw new IllegalStateException("transaction is not running");
        }
    }

    private void desireInactive() {
        if (isTransactionActive()) {
            throw new IllegalStateException("transaction is running");
        }
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        try (var t = transaction; var c = client) {
            return;
        }
    }
}
