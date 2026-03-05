/*
 * Copyright 2023-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.tsurugidb.tsubakuro.test.util.DbTester;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

class DbResultSetTest extends DbTester {

    private static int SIZE = 10_0000;

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
    void select0() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var rs = transaction.executeQuery(sql).await(10, TimeUnit.SECONDS)) {
                // do nothing
            }
        });
    }

    @RepeatedTest(100)
    void select1() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var rs = transaction.executeQuery(sql).await(10, TimeUnit.SECONDS)) {
                assertTrue(rs.nextRow());
            }
        });
    }

    @RepeatedTest(10)
    void selectAll() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var rs = transaction.executeQuery(sql).await(10, TimeUnit.SECONDS)) {
                while (rs.nextRow()) {
                    // do nothing
                }
            }
        });
    }

    @Test
    void getAfterClose() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var rs = transaction.executeQuery(sql).await()) {
                rs.close();
                var e = assertThrowsExactly(IOException.class, () -> {
                    rs.nextRow();
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        });
    }

    @Test
    void getAfterClose_timeout() throws Exception {
        executeOcc(transaction -> {
            var sql = "select * from test";
            try (var rs = transaction.executeQuery(sql).await(3, TimeUnit.SECONDS)) {
                rs.setCloseTimeout(new Timeout(3, TimeUnit.SECONDS, Policy.ERROR));
                rs.close();
                var e = assertThrowsExactly(IOException.class, () -> {
                    rs.nextRow();
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        });
    }
}
