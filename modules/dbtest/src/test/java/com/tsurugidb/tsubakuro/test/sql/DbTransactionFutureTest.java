package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.tsubakuro.common.ShutdownType;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.test.util.DbTestConnector;
import com.tsurugidb.tsubakuro.test.util.DbTester;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

class DbTransactionFutureTest extends DbTester {

//    @BeforeAll
//    static void beforeAll(TestInfo info) throws Exception {
//        var LOG = LoggerFactory.getLogger(DbTransactionFutureTest.class);
//        logInitStart(LOG, info);
//
//        dropTableIfExists("test");
//        var sql = "create table test (\n" //
//                + "  pk int primary key,\n" //
//                + "  long_value bigint,\n" //
//                + "  string_value varchar(10)\n" //
//                + ")";
//        executeDdl(sql);
//
//        logInitEnd(LOG, info);
//    }

    @RepeatedTest(6)
    void doNothing() throws Exception {
        doNothing(null);
    }

    @RepeatedTest(6)
    void doNothing_GRACEFUL() throws Exception {
        doNothing(ShutdownType.GRACEFUL);
    }

    @RepeatedTest(6)
    void doNothing_FORCEFUL() throws Exception {
        doNothing(ShutdownType.FORCEFUL);
    }

    void doNothing(ShutdownType shutdownType) throws Exception {
        try (var session = DbTestConnector.createSession("DbTransactionFutureTest.doNothing().main"); //
                var sqlClient = SqlClient.attach(session)) {
            for (int i = 0; i < 300; i++) {
                var option = TransactionOption.newBuilder() //
                        .setType(TransactionType.SHORT) //
                        .setLabel("DbTransactionFutureTest.doNothing().main" + i) //
                        .build();
                try (var future = sqlClient.createTransaction(option)) {
                    // do nothing
                }
            }
            if (shutdownType != null) {
                session.shutdown(shutdownType).await();
            }
        }

        try (var session = DbTestConnector.createSession("DbTransactionFutureTest.doNothing().after"); //
                var sqlClient = SqlClient.attach(session)) {
            var option = TransactionOption.newBuilder() //
                    .setType(TransactionType.SHORT) //
                    .setLabel("DbTransactionFutureTest.doNothing().after") //
                    .build();
            try (var transaction = sqlClient.createTransaction(option).await()) {
                transaction.executeStatement("drop table if exists test").await();
                transaction.commit().await();
            }
        }
    }

    @Test
    void getAfterClose() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            var option = TransactionOption.newBuilder() //
                    .setType(TransactionType.SHORT) //
                    .setLabel("DbTransactionFutureTest.getAfterClose") //
                    .build();
            try (var future = sqlClient.createTransaction(option)) {
                future.close();
                var e = assertThrowsExactly(IOException.class, () -> {
                    future.get();
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        }
    }

    @Test
    void getAfterClose_timeout() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            var option = TransactionOption.newBuilder() //
                    .setType(TransactionType.SHORT) //
                    .setLabel("DbTransactionFutureTest.getAfterClose_timeout") //
                    .build();
            try (var future = sqlClient.createTransaction(option)) {
                future.setCloseTimeout(new Timeout(3, TimeUnit.SECONDS, Policy.ERROR));
                future.close();
                var e = assertThrowsExactly(IOException.class, () -> {
                    future.get(3, TimeUnit.SECONDS);
                });
                assertTrue(e.getMessage().contains("already closed"));
            }
        }
    }
}
