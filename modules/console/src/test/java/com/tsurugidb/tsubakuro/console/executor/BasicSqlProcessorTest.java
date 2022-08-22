package com.tsurugidb.tsubakuro.console.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.impl.low.sql.ResultSetMetadataAdapter;
import com.nautilus_technologies.tsubakuro.impl.low.sql.testing.Relation;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SqlClient;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.tateyama.proto.SqlRequest;
import com.tsurugidb.tateyama.proto.SqlResponse;
import com.tsurugidb.tsubakuro.console.model.Region;

class BasicSqlProcessorTest {

    @Test
    void startTransaction() throws Exception {
        Transaction tx = new Transaction() {
            // nothing special
        };
        SqlClient client = new SqlClient() {
            @Override
            public FutureResponse<Transaction> createTransaction(SqlRequest.TransactionOption option) throws IOException {
                return FutureResponse.returns(tx);
            }
        };
        try (var sql = new BasicSqlProcessor(client)) {
            assertFalse(sql.isTransactionActive());
            sql.startTransaction(SqlRequest.TransactionOption.getDefaultInstance());
            assertTrue(sql.isTransactionActive());
            assertSame(tx, sql.getTransaction());
        }
    }

    @Test
    void startTransaction_active_tx() throws Exception {
        Transaction tx = new Transaction() {
            // nothing special
        };
        SqlClient client = new SqlClient() {
            @Override
            public FutureResponse<Transaction> createTransaction(SqlRequest.TransactionOption option) throws IOException {
                return FutureResponse.returns(tx);
            }
        };
        try (var sql = new BasicSqlProcessor(client)) {
            sql.startTransaction(SqlRequest.TransactionOption.getDefaultInstance());
            assertThrows(IllegalStateException.class,
                    () -> sql.startTransaction(SqlRequest.TransactionOption.getDefaultInstance()));
        }
    }

    @Test
    void commitTransaction() throws Exception {
        var reached = new AtomicBoolean();
        Transaction tx = new Transaction() {
            @Override
            public FutureResponse<Void> commit(SqlRequest.CommitStatus status) throws IOException {
                if (!reached.compareAndSet(false, true)) {
                    throw new AssertionError();
                }
                assertEquals(SqlRequest.CommitStatus.STORED, status);
                return FutureResponse.returns(null);
            }
        };
        SqlClient client = new SqlClient() {
            @Override
            public FutureResponse<Transaction> createTransaction(SqlRequest.TransactionOption option) throws IOException {
                return FutureResponse.returns(tx);
            }
        };
        try (var sql = new BasicSqlProcessor(client)) {
            assertFalse(sql.isTransactionActive());
            sql.startTransaction(SqlRequest.TransactionOption.getDefaultInstance());
            assertTrue(sql.isTransactionActive());
            sql.commitTransaction(SqlRequest.CommitStatus.STORED);
            assertFalse(sql.isTransactionActive());
        }
        assertTrue(reached.get());
    }

    @Test
    void commitTransaction_inactive_tx() throws Exception {
        SqlClient client = new SqlClient() {
            // nothing special
        };
        try (var sql = new BasicSqlProcessor(client)) {
            assertThrows(IllegalStateException.class, () -> sql.commitTransaction(null));
        }
    }

    @Test
    void rollbackTransaction() throws Exception {
        var reached = new AtomicBoolean();
        Transaction tx = new Transaction() {
            @Override
            public FutureResponse<Void> rollback() throws IOException {
                if (!reached.compareAndSet(false, true)) {
                    throw new AssertionError();
                }
                return FutureResponse.returns(null);
            }
        };
        SqlClient client = new SqlClient() {
            @Override
            public FutureResponse<Transaction> createTransaction(SqlRequest.TransactionOption option) throws IOException {
                return FutureResponse.returns(tx);
            }
        };
        try (var sql = new BasicSqlProcessor(client)) {
            assertFalse(sql.isTransactionActive());
            sql.startTransaction(SqlRequest.TransactionOption.getDefaultInstance());
            assertTrue(sql.isTransactionActive());
            sql.rollbackTransaction();
            assertFalse(sql.isTransactionActive());
        }
        assertTrue(reached.get());
    }

    @Test
    void rollbackTransaction_inactive_tx() throws Exception {
        SqlClient client = new SqlClient() {
            // nothing special
        };
        try (var sql = new BasicSqlProcessor(client)) {
            sql.rollbackTransaction();
        }
    }

    @Test
    void execute_wo_result() throws Exception {
        var reached = new AtomicBoolean();
        PreparedStatement ps = createPreparedStatement(false);
        Transaction tx = new Transaction() {
            @Override
            public FutureResponse<Void> executeStatement(
                    PreparedStatement statement,
                    Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
                if (!reached.compareAndSet(false, true)) {
                    throw new AssertionError();
                }
                assertSame(ps, statement);
                return FutureResponse.returns(null);
            }
        };
        SqlClient client = new SqlClient() {
            @Override
            public FutureResponse<Transaction> createTransaction(SqlRequest.TransactionOption option) throws IOException {
                return FutureResponse.returns(tx);
            }
            @Override
            public FutureResponse<PreparedStatement> prepare(
                    String source,
                    Collection<? extends SqlRequest.Placeholder> placeholders) throws IOException {
                return FutureResponse.returns(ps);
            }
        };
        try (var sql = new BasicSqlProcessor(client)) {
            sql.startTransaction(SqlRequest.TransactionOption.getDefaultInstance());
            try (var rs = sql.execute("", new Region(0, 0, 0, 0))) {
                assertNull(rs);
            }
        }
        assertTrue(reached.get());
    }

    @Test
    void execute_w_result() throws Exception {
        var reached = new AtomicBoolean();
        ResultSet r = Relation.of()
                .getResultSet(new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.getDefaultInstance()));
        PreparedStatement ps = createPreparedStatement(true);
        Transaction tx = new Transaction() {
            @Override
            public FutureResponse<ResultSet> executeQuery(
                    PreparedStatement statement,
                    Collection<? extends SqlRequest.Parameter> parameters) throws IOException {
                if (!reached.compareAndSet(false, true)) {
                    throw new AssertionError();
                }
                assertSame(ps, statement);
                return FutureResponse.returns(r);
            }
        };
        SqlClient client = new SqlClient() {
            @Override
            public FutureResponse<Transaction> createTransaction(SqlRequest.TransactionOption option) throws IOException {
                return FutureResponse.returns(tx);
            }
            @Override
            public FutureResponse<PreparedStatement> prepare(
                    String source,
                    Collection<? extends SqlRequest.Placeholder> placeholders) throws IOException {
                return FutureResponse.returns(ps);
            }
        };
        try (var sql = new BasicSqlProcessor(client)) {
            sql.startTransaction(SqlRequest.TransactionOption.getDefaultInstance());
            try (var rs = sql.execute("", new Region(0, 0, 0, 0))) {
                assertSame(r, rs);
            }
        }
        assertTrue(reached.get());
    }

    @Test
    void execute_inactive_tx() throws Exception {
        SqlClient client = new SqlClient() {
            // nothing special
        };
        try (var sql = new BasicSqlProcessor(client)) {
            assertThrows(IllegalStateException.class, () -> sql.execute("", new Region(0, 0, 0, 0)));
        }
    }

    private static PreparedStatement createPreparedStatement(boolean hasResult) {
        return new PreparedStatement() {

            @Override
            public void setCloseTimeout(long timeout, TimeUnit unit) {
                return;
            }

            @Override
            public boolean hasResultRecords() {
                return hasResult;
            }

            @Override
            public void close() {
                return;
            }
        };
    }
}
