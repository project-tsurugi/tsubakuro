package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.test.util.DbTester;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class DbResultSet2Test extends DbTester {

    private static int SIZE = 10;

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
    void selectFutureClose_select() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var rsFuture1 = transaction.executeQuery(sql)) {
                rsFuture1.close();

                try (var rs2 = transaction.executeQuery(sql).await(5, TimeUnit.SECONDS)) {
                    assertTrue(rs2.nextRow());
                }
            }
        });
    }

    @RepeatedTest(10)
    void selectClose_select() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var rs1 = transaction.executeQuery(sql).await(10, TimeUnit.SECONDS)) {
                rs1.close();

                try (var rs2 = transaction.executeQuery(sql).await(10, TimeUnit.SECONDS)) {
                    assertTrue(rs2.nextRow());
                }
            }
        });
    }
}
