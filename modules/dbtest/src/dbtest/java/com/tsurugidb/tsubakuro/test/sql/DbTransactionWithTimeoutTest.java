package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.RepeatedTest;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.common.ShutdownType;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.test.util.DbTestConnector;
import com.tsurugidb.tsubakuro.test.util.DbTester;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class DbTransactionWithTimeoutTest extends DbTester {

    @RepeatedTest(5)
    void timeout_nothing() throws Exception {
        new Test() {
            @Override
            protected Session createSession(SessionBuilder builder) throws IOException, ServerException, InterruptedException {
                return builder.create();
            }

            @Override
            protected <V> V get(FutureResponse<V> future) throws IOException, ServerException, InterruptedException {
                return future.await();
            }
        }.execute();
    }

    @RepeatedTest(5)
    void timeout0() throws Exception {
        int timeout = 0;

        new Test() {
            @Override
            protected Session createSession(SessionBuilder builder) throws IOException, ServerException, InterruptedException, TimeoutException {
                return builder.create(timeout, TimeUnit.SECONDS);
            }

            @Override
            protected <V> V get(FutureResponse<V> future) throws IOException, ServerException, InterruptedException, TimeoutException {
                return future.await(timeout, TimeUnit.SECONDS);
            }
        }.execute();
    }

    @RepeatedTest(5)
    void timeout3() throws Exception {
        int timeout = 3;

        new Test() {
            @Override
            protected Session createSession(SessionBuilder builder) throws IOException, ServerException, InterruptedException, TimeoutException {
                return builder.create(timeout, TimeUnit.SECONDS);
            }

            @Override
            protected <V> V get(FutureResponse<V> future) throws IOException, ServerException, InterruptedException, TimeoutException {
                return future.await(timeout, TimeUnit.SECONDS);
            }
        }.execute();
    }

    private static abstract class Test {
        public void execute() throws Exception {
            var builder = DbTestConnector.createSessionBuilder();
            try (var session = createSession(builder); //
                    var sqlClient = SqlClient.attach(session)) {
                executeAndCommit(sqlClient, transaction -> {
                    String sql = "drop table if exists test";
                    get(transaction.executeStatement(sql));
                });
                executeAndCommit(sqlClient, transaction -> {
                    String sql = "create table test (\n" //
                            + "  pk int primary key,\n" //
                            + "  long_value bigint,\n" //
                            + "  string_value varchar(10)\n" //
                            + ")";
                    get(transaction.executeStatement(sql));
                });

                executeAndCommit(sqlClient, transaction -> {
                    for (int i = 0; i < 4; i++) {
                        String sql = String.format("insert into test values(%d, %d, '%d')", i, i, i);
                        get(transaction.executeStatement(sql));
                    }
                });

                executeAndCommit(sqlClient, transaction -> {
                    String sql = "select * from test order by pk";
                    try (var rs = get(transaction.executeQuery(sql))) {
                        for (int i = 0; i < 4; i++) {
                            assertTrue(rs.nextRow());

                            assertTrue(rs.nextColumn());
                            assertEquals(i, rs.fetchInt4Value());
                            assertTrue(rs.nextColumn());
                            assertEquals(i, rs.fetchInt8Value());
                            assertTrue(rs.nextColumn());
                            assertEquals(Integer.toString(i), rs.fetchCharacterValue());

                            assertFalse(rs.nextColumn());
                        }
                        assertFalse(rs.nextRow());
                    }
                });

                get(session.shutdown(ShutdownType.GRACEFUL));
            }
        }

        @FunctionalInterface
        private interface Action {
            void execute(Transaction transaction) throws IOException, InterruptedException, ServerException, TimeoutException;
        }

        private void executeAndCommit(SqlClient sqlClient, Action action) throws IOException, InterruptedException, ServerException, TimeoutException {
            try (var transaction = get(sqlClient.createTransaction())) {
                try {
                    action.execute(transaction);
                    get(transaction.commit());
                } catch (Throwable e) {
                    try {
                        get(transaction.rollback());
                    } catch (Throwable s) {
                        e.addSuppressed(s);
                    }
                    throw e;
                }
            }
        }

        protected abstract Session createSession(SessionBuilder builder) throws IOException, ServerException, InterruptedException, TimeoutException;

        protected abstract <V> V get(FutureResponse<V> future) throws IOException, ServerException, InterruptedException, TimeoutException;
    }
}
