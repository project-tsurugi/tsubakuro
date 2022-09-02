package com.tsurugidb.tsubakuro.console.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.impl.ResultSetMetadataAdapter;
import com.tsurugidb.tsubakuro.sql.impl.testing.Relation;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.console.model.Region;
import com.tsurugidb.tsubakuro.console.model.Statement;
import com.tsurugidb.tsubakuro.console.parser.SqlParser;

class BasicEngineTest {

    static class MockSqlProcessor implements SqlProcessor {

        private boolean active = false;

        MockSqlProcessor() {
            this(false);
        }

        MockSqlProcessor(boolean active) {
            this.active = active;
        }

        @Override
        public boolean isTransactionActive() {
            return active;
        }

        @Override
        public ResultSet execute(String statement, Region region)
                throws ServerException, IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startTransaction(SqlRequest.TransactionOption option)
                throws ServerException, IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commitTransaction(SqlRequest.CommitStatus status) throws ServerException, IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rollbackTransaction() throws ServerException, IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }
    }

    static class MockResultProcessor implements ResultProcessor {

        @Override
        public void process(ResultSet target) throws ServerException, IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void empty_statement() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor();
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse(";"));
        assertTrue(cont);
    }

    @Test
    void generic_staement_wo_result() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(true) {
            @Override
            public ResultSet execute(String statement, Region region) {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals("INSERT INTO A DEFAULT VALUES", statement);
                return null;
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("INSERT INTO A DEFAULT VALUES"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void generic_staement_w_result() throws Exception {
        var reachedExec = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(true) {
            @Override
            public ResultSet execute(String statement, Region region) {
                if (!reachedExec.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals("SELECT * FROM T", statement);
                return Relation.of(new Object[][] { { 1 } })
                        .getResultSet(new ResultSetMetadataAdapter(SqlResponse.ResultSetMetadata.newBuilder()
                                .addColumns(Types.column(int.class))
                                .build()));
            }
        };
        var reachedRs = new AtomicBoolean();
        MockResultProcessor rs = new MockResultProcessor() {
            @Override
            public void process(ResultSet target) throws ServerException, IOException, InterruptedException {
                if (!reachedRs.compareAndSet(false, true)) {
                    fail();
                }
                assertTrue(target.nextRow());
                assertTrue(target.nextColumn());
                assertEquals(1, target.fetchInt4Value());
                assertFalse(target.nextColumn());
                assertFalse(target.nextRow());
            }
        };
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("SELECT * FROM T"));
        assertTrue(cont);
        assertTrue(reachedExec.get());
        assertTrue(reachedRs.get());
    }

    @Test
    void call_staement_fall_through() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(true) {
            @Override
            public ResultSet execute(String statement, Region region) {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals("CALL proc()", statement);
                return null;
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("CALL proc()"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void generic_staement_inactive_tx() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        assertThrows(EngineException.class, () -> engine.execute(parse("SELECT * FROM T")));
    }

    @Test
    void start_transaction_statement() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(false) {
            @Override
            public void startTransaction(SqlRequest.TransactionOption option)
                    throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals(SqlRequest.TransactionType.TRANSACTION_TYPE_UNSPECIFIED, option.getType());
                assertEquals(SqlRequest.TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, option.getPriority());
                assertEquals("", option.getLabel());
                assertEquals(0, option.getWritePreservesCount());
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("START TRANSACTION"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void start_transaction_statement_long() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(false) {
            @Override
            public void startTransaction(SqlRequest.TransactionOption option)
                    throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals(SqlRequest.TransactionType.LONG, option.getType());
                assertEquals(SqlRequest.TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, option.getPriority());
                assertEquals("", option.getLabel());
                assertEquals(0, option.getWritePreservesCount());
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("START LONG TRANSACTION"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void start_transaction_statement_prior() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(false) {
            @Override
            public void startTransaction(SqlRequest.TransactionOption option)
                    throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals(SqlRequest.TransactionType.TRANSACTION_TYPE_UNSPECIFIED, option.getType());
                assertEquals(SqlRequest.TransactionPriority.WAIT, option.getPriority());
                assertEquals("", option.getLabel());
                assertEquals(0, option.getWritePreservesCount());
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("START TRANSACTION EXECUTE PRIOR"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void start_transaction_statement_as() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(false) {
            @Override
            public void startTransaction(SqlRequest.TransactionOption option)
                    throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals(SqlRequest.TransactionType.TRANSACTION_TYPE_UNSPECIFIED, option.getType());
                assertEquals(SqlRequest.TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, option.getPriority());
                assertEquals("TESTING", option.getLabel());
                assertEquals(0, option.getWritePreservesCount());
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("START TRANSACTION AS TESTING"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void start_transaction_statement_write_preserve() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(false) {
            @Override
            public void startTransaction(SqlRequest.TransactionOption option)
                    throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals(SqlRequest.TransactionType.LONG, option.getType());
                assertEquals(SqlRequest.TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, option.getPriority());
                assertEquals("", option.getLabel());
                assertEquals(3, option.getWritePreservesCount());
                assertEquals("a", option.getWritePreserves(0).getTableName());
                assertEquals("b", option.getWritePreserves(1).getTableName());
                assertEquals("c", option.getWritePreserves(2).getTableName());
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("START TRANSACTION WRITE PRESERVE a, b, c"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void start_transaction_statement_tx_active() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(true);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        assertThrows(EngineException.class, () -> engine.execute(parse("START TRANSACTION")));
    }

    @Test
    void commit_statement() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(true) {
            @Override
            public void commitTransaction(SqlRequest.CommitStatus status)
                    throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals(null, status);
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("COMMIT"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void commit_statement_stored() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(true) {
            @Override
            public void commitTransaction(SqlRequest.CommitStatus status)
                    throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
                assertEquals(SqlRequest.CommitStatus.STORED, status);
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("COMMIT WAIT STORED"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void commit_statement_tx_inactive() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        assertThrows(EngineException.class, () -> engine.execute(parse("COMMIT")));
    }

    @Test
    void rollback_statement() throws Exception {
        var reached = new AtomicBoolean();
        MockSqlProcessor sql = new MockSqlProcessor(true) {
            @Override
            public void rollbackTransaction() throws ServerException, IOException, InterruptedException {
                if (!reached.compareAndSet(false, true)) {
                    fail();
                }
            }
        };
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("ROLLBACK"));
        assertTrue(cont);
        assertTrue(reached.get());
    }

    @Test
    void rollback_statement_tx_inactive() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        assertThrows(EngineException.class, () -> engine.execute(parse("COMMIT")));
    }

    @Test
    void special_statement_exit() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("\\exit"));
        assertFalse(cont);
    }

    @Test
    void special_statement_halt() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("\\halt"));
        assertFalse(cont);
    }

    @Test
    void special_statement_status() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("\\status"));
        assertTrue(cont);
    }

    @Test
    void special_statement_status_tx_active() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(true);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("\\status"));
        assertTrue(cont);
    }

    @Test
    void special_statement_help() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("\\help"));
        assertTrue(cont);
    }

    @Test
    void special_statement_exit_tx_active() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(true);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        assertThrows(EngineException.class, () -> engine.execute(parse("\\exit")));
    }

    @Test
    void special_statement_halt_tx_active() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(true);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        var cont = engine.execute(parse("\\halt"));
        assertFalse(cont);
    }

    @Test
    void special_statement_unknown() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        assertThrows(EngineException.class, () -> engine.execute(parse("\\UNKNOWN_COMMAND")));
    }

    @Test
    void erroneous_statement_unknown() throws Exception {
        MockSqlProcessor sql = new MockSqlProcessor(false);
        MockResultProcessor rs = new MockResultProcessor();
        var engine = new BasicEngine(sql, rs);
        assertThrows(EngineException.class, () -> engine.execute(parse("START TRANSACTION INVALID")));
    }

    private static Statement parse(String text) throws IOException {
        try (var parser = new SqlParser(new StringReader(text))) {
            return parser.next();
        }
    }
}
