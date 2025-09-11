package com.tsurugidb.tsubakuro.test.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.test.util.DbTestConnector;
import com.tsurugidb.tsubakuro.test.util.DbTester;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

class DbSessionBuilderTest extends DbTester {

    private static final int ATTEMPT_SIZE = 400;

    private static SessionBuilder createSessionBuilder() {
        var builder = SessionBuilder.connect(DbTestConnector.getEndPoint()) //
                .withApplicationName("DbSessionBuilderTest");
        return builder;
    }

    @Test
    void create() throws Exception {
        var builder = createSessionBuilder();
        builder.withLabel("create");

        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            try (var session = builder.create()) {
                // close only
                session.close();
            }
        }
    }

    @Test
    void create_isAlive() throws Exception {
        var builder = createSessionBuilder();
        builder.withLabel("create_isAlive");

        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            try (var session = builder.create()) {
                assertTrue(session.isAlive());
                session.close();
                assertFalse(session.isAlive());
            }
        }
    }

    @Test
    void createTimeout() throws Exception {
        createTimeout(1);
    }

    @Test
    void createTimeout0() throws Exception {
        createTimeout(0);
    }

    private void createTimeout(int timeout) throws Exception {
        var builder = createSessionBuilder();
        builder.withLabel("createTimeout");

        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            try (var session = builder.create(timeout, TimeUnit.SECONDS)) {
                // close only
                session.setCloseTimeout(new Timeout(timeout, TimeUnit.SECONDS, Policy.ERROR));
                session.close();
            }
        }
    }

    @Test
    void createAsync() throws Exception {
        var builder = createSessionBuilder();
        builder.withLabel("createAsync");

        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            try (var future = builder.createAsync()) {
                // close only
                future.setCloseTimeout(new Timeout(1, TimeUnit.SECONDS, Policy.ERROR));
                future.close();
                assertFalse(future.isDone());
            }
        }
    }

    @Test
    void createAsync_get() throws Exception {
        var builder = createSessionBuilder();
        builder.withLabel("createAsync_get");

        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            try (var future = builder.createAsync()) {
                assertFalse(future.isDone());
                try (var session = future.get(1, TimeUnit.SECONDS)) {
                    // close only
                    session.setCloseTimeout(new Timeout(1, TimeUnit.SECONDS, Policy.ERROR));
                }
                assertTrue(future.isDone());
            }
        }
    }
}
