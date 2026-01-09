package com.tsurugidb.tsubakuro.test.common;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.test.util.DbTestConnector;
import com.tsurugidb.tsubakuro.test.util.DbTester;

/**
 * connect Long.MAX_VALUE timeout test.
 */
public class DbConnectMaxTimeoutTest extends DbTester {

    @Test
    void nanoseconds() throws Exception {
        test(TimeUnit.NANOSECONDS);
    }

    @Test
    void microseconds() throws Exception {
        test(TimeUnit.MICROSECONDS);
    }

    @Test
    void milliseconds() throws Exception {
        test(TimeUnit.MILLISECONDS);
    }

    @Test
    void seconds() throws Exception {
        test(TimeUnit.SECONDS);
    }

    @Test
    void mitutes() throws Exception {
        test(TimeUnit.MINUTES);
    }

    @Test
    void hours() throws Exception {
        test(TimeUnit.HOURS);
    }

    @Test
    void days() throws Exception {
        test(TimeUnit.DAYS);
    }

    private void test(TimeUnit unit) throws ServerException, IOException, InterruptedException, TimeoutException {
        var endpoint = DbTestConnector.getEndPoint();

        var list = new ArrayList<Session>();
        try {
            for (int i = 0; i < 3; i++) {
                try (var future = SessionBuilder.connect(endpoint).createAsync();
                        var session = future.get(Long.MAX_VALUE, unit); //
                        var sqlClient = SqlClient.attach(session)) {
                    list.add(session);

                    try (var tx = sqlClient.createTransaction().await()) {
                        tx.executeQuery("select * from test").await();
                    } catch (Exception e) {
                        LOG.info("{}: {}", i, e.getMessage());
                    }
                    assertTrue(session.isAlive());
                }
            }
        } finally {
            for (var session : list) {
                try {
                    ((SessionImpl) session).waitForCompletion();
                } catch (Exception e) {
                    LOG.error("waitForCompletion error", e);
                }
            }
        }
    }
}
