package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.exception.InactiveTransactionException;
import com.tsurugidb.tsubakuro.sql.exception.RestrictedOperationException;
import com.tsurugidb.tsubakuro.test.util.DbTester;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

class DbResultSetFutureTest extends DbTester {

    private static int SIZE = 10000;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbTransactionFutureTest.class);
        logInitStart(LOG, info);

        dropTableIfExists("test");
        var sql = "create table test (\n" //
                + "  pk int primary key,\n" //
                + "  long_value bigint,\n" //
                + "  string_value varchar(10)\n" //
                + ")";
        executeDdl(sql);
        insert(SIZE);

        logInitEnd(LOG, info);
    }

    private static void insert(int size) throws ServerException, IOException, InterruptedException, TimeoutException {
        var sb = new StringBuilder(1024);

        executeOcc(transaction -> {
            var futureList = new ArrayList<FutureResponse<ExecuteResult>>();

            int c = 0;
            for (int i = 0; i < size; i++) {
                if (c == 0) {
                    sb.append("insert or replace into test values");
                } else {
                    sb.append(",");
                }
                sb.append("(");
                sb.append(i);
                sb.append(",");
                sb.append(i);
                sb.append(",'");
                sb.append(i);
                sb.append("')");

                c++;
                if (c >= 2000 || i == size - 1) {
                    var sql = sb.toString();
                    var future = transaction.executeStatement(sql);
                    futureList.add(future);
                    sb.setLength(0);
                    c = 0;
                }
            }

            for (var future : futureList) {
                future.await(10, TimeUnit.SECONDS);
            }
        });
    }

    @RepeatedTest(10)
    void doNothing() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var future = transaction.executeQuery(sql)) {
                // do nothing
            }
        }, e -> {
            if (e instanceof InactiveTransactionException) {
                if (e.getMessage().contains("the other request already made to terminate the transaction")) {
                    return;
                }
            }
            if (e instanceof RestrictedOperationException) {
                if (e.getMessage().contains("commit requested while other transaction operations are on-going")) {
                    return;
                }
            }
            throw e;
        });
    }

    @Test
    void getAfterClose() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var future = transaction.executeQuery(sql)) {
                future.close();
                var e = assertThrowsExactly(IOException.class, () -> {
                    future.get();
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        }, e -> {
            if (e instanceof InactiveTransactionException) {
                if (e.getMessage().contains("the other request already made to terminate the transaction")) {
                    return;
                }
            }
            throw e;
        });
    }

    @Test
    void getAfterClose_timeout() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var future = transaction.executeQuery(sql)) {
                future.setCloseTimeout(new Timeout(3, TimeUnit.SECONDS, Policy.ERROR));
                future.close();
                var e = assertThrowsExactly(IOException.class, () -> {
                    future.get(3, TimeUnit.SECONDS);
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        }, e -> {
            if (e instanceof InactiveTransactionException) {
                if (e.getMessage().contains("the other request already made to terminate the transaction")) {
                    return;
                }
            }
            throw e;
        });
    }
}
