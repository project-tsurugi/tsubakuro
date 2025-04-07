package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.exception.ConflictOnWritePreserveException;
import com.tsurugidb.tsubakuro.sql.exception.UniqueConstraintViolationException;
import com.tsurugidb.tsubakuro.test.util.DbTestConnector;
import com.tsurugidb.tsubakuro.test.util.DbTester;

class DbTransactionTest extends DbTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbTransactionTest.class);
        logInitStart(LOG, info);

        dropTableIfExists("test");
        var sql = "create table test (\n" //
                + "  pk int primary key,\n" //
                + "  long_value bigint,\n" //
                + "  string_value varchar(10)\n" //
                + ")";
        executeDdl(sql);
        executeOcc(transaction -> {
            for (int i = 0; i < 4; i++) {
                insert(transaction, i);
            }
        });

        logInitEnd(LOG, info);
    }

    static void insert(Transaction transaction, int i) throws IOException, ServerException, InterruptedException, TimeoutException {
        var sql = String.format("insert into test values(%d, %d, '%d')", i, i, i);
        transaction.executeStatement(sql).await(10, TimeUnit.SECONDS);
    }

    @RepeatedTest(30)
    void occAfterLtx() throws Exception {
        try (var session = DbTestConnector.createSession("DbTransactionTest.occAfterLtx"); //
                var sqlClient = SqlClient.attach(session)) {
            var ltxOption = TransactionOption.newBuilder() //
                    .setType(TransactionType.LONG) //
                    .addWritePreserves(WritePreserve.newBuilder().setTableName("test")) //
                    .build();
            try (var ltx = sqlClient.createTransaction(ltxOption).await(10, TimeUnit.SECONDS)) {
                ltx.setCloseTimeout(10, TimeUnit.SECONDS);

                var occOption = TransactionOption.newBuilder() //
                        .setType(TransactionType.SHORT) //
                        .build();
                try (var occ = sqlClient.createTransaction(occOption).await(10, TimeUnit.SECONDS)) {
                    occ.setCloseTimeout(10, TimeUnit.SECONDS);

                    assertThrowsExactly(ConflictOnWritePreserveException.class, () -> {
                        insert(occ, 1);
                    });
                    var e = occ.getSqlServiceException().await(10, TimeUnit.SECONDS);
                    assertInstanceOf(ConflictOnWritePreserveException.class, e);

                    occ.rollback().await(10, TimeUnit.SECONDS);
                }
            }

            var occOption = TransactionOption.newBuilder() //
                    .setType(TransactionType.SHORT) //
                    .build();
            try (var occ = sqlClient.createTransaction(occOption).await(10, TimeUnit.SECONDS)) {
                occ.setCloseTimeout(10, TimeUnit.SECONDS);

                assertThrowsExactly(UniqueConstraintViolationException.class, () -> {
                    insert(occ, 1);
                });
                var e = occ.getSqlServiceException().await(10, TimeUnit.SECONDS);
                assertInstanceOf(UniqueConstraintViolationException.class, e);

                occ.rollback().await(10, TimeUnit.SECONDS);
            }
        }
    }
}
