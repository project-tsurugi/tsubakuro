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
