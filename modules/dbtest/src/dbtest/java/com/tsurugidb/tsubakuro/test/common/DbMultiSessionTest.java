package com.tsurugidb.tsubakuro.test.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.test.util.DbTestConnector;
import com.tsurugidb.tsubakuro.test.util.DbTester;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * multiple session test.
 */
class DbMultiSessionTest extends DbTester {

    private static final int ATTEMPT_SIZE = 260;

    private static final int EXPECTED_SESSION_SIZE = getSystemProperty("tsurugi.dbtest.expected.session.size", 104);

    @RepeatedTest(10)
    void limit() throws Exception {
        closeStaticSession();

        var sessionList = new ArrayList<Session>();
        Throwable occurred = null;
        try {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                Session session;
                try {
                    session = DbTestConnector.createSession0(DbTestConnector.getSessionLabel() + ".session" + i);
                } catch (IOException e) {
                    assertEquals("the server has declined the connection request", e.getMessage());
                    int count = sessionList.size();
                    if (count < EXPECTED_SESSION_SIZE) {
                        fail(MessageFormat.format("less session.size expected: {1} but was: {0}", count, EXPECTED_SESSION_SIZE));
                    }
                    return;
                }
                sessionList.add(session);
            }

            limit(sessionList);
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            var exceptionList = new ArrayList<Exception>();
            for (var session : sessionList) {
                try {
                    session.close();
                } catch (Exception e) {
                    exceptionList.add(e);
                }
            }
            if (!exceptionList.isEmpty()) {
                var e = new Exception("session close error. errorCount=" + exceptionList.size());
                exceptionList.forEach(e::addSuppressed);
                if (occurred != null) {
                    occurred.addSuppressed(e);
                } else {
                    throw e;
                }
            }
        }
    }

    private void limit(List<Session> list) {
        int count = 0;
        for (var session : list) {
            if (session.isAlive()) {
                count++;
            }
        }
        if (count < EXPECTED_SESSION_SIZE) {
            fail(MessageFormat.format("less session.size expected: {1} but was: {0}", count, EXPECTED_SESSION_SIZE));
        }
    }

    @Test
    void manySession1() throws Exception {
        manySession(false, false);
    }

    @Test
    void manySession2() throws Exception {
        manySession(true, false);
    }

    @Test
    void manySession3() throws Exception {
        manySession(false, true);
    }

    private void manySession(boolean getSession, boolean transaction) throws Exception {
        LOG.debug("create session start");
        var futureList = new ArrayList<FutureResponse<? extends Session>>();
        var sessionMap = new HashMap<FutureResponse<? extends Session>, Session>();
        var clientMap = new HashMap<FutureResponse<? extends Session>, SqlClient>();
        try {
            for (int i = 0; i < 60; i++) {
                var future = DbTestConnector.createSessionFuture(DbTestConnector.getSessionLabel() + ".session" + i);
                futureList.add(future);

                if (getSession) {
                    var session = future.get(20, TimeUnit.SECONDS);
                    sessionMap.put(future, session);
                    var sqlClient = SqlClient.attach(session);
                    clientMap.put(future, sqlClient);
                }
            }
            LOG.debug("create session end");

            if (transaction) {
                LOG.debug("createTransaction start");
                int i = 0;
                for (var future : futureList) {
                    var sqlClient = clientMap.get(future);
                    if (sqlClient == null) {
                        var session = future.get(20, TimeUnit.SECONDS);
                        sessionMap.put(future, session);
                        sqlClient = SqlClient.attach(session);
                        clientMap.put(future, sqlClient);
                    }

                    LOG.debug("createTransaction {}", i);
                    var option = createTransactionOptionOcc("manySession" + i);
                    try (var tx = sqlClient.createTransaction(option).await(20, TimeUnit.SECONDS)) {
                    }
                    i++;
                }
                LOG.debug("createTransaction end");
            }
        } finally {
            LOG.debug("close session start");
            close(futureList, clientMap.values(), sessionMap.values());
            LOG.debug("close session end");
        }
    }

    @Test
    void multiThread() {
        LOG.debug("create session start");
        var sessionList = new CopyOnWriteArrayList<Session>();
        try {
            var threadList = new ArrayList<Thread>();
            var alive = new AtomicBoolean(true);
            for (int i = 0; i < 60; i++) {
                var thread = new Thread(() -> {
                    Session session;
                    try {
                        session = DbTestConnector.createSession();
                    } catch (Exception e) {
                        LOG.warn("connect error. {}: {}", e.getClass().getName(), e.getMessage());
                        return;
                    }
                    sessionList.add(session);

                    while (alive.get()) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                threadList.add(thread);
                thread.start();
            }
            LOG.debug("create session end");

            alive.set(false);

            LOG.debug("thread join start");
            for (var thread : threadList) {
                try {
                    thread.join();
                } catch (Exception e) {
                    LOG.warn("join error", e);
                }
            }
            LOG.debug("thread join end");
        } finally {
            LOG.debug("close session start");
            for (var session : sessionList) {
                try {
                    session.close();
                } catch (Exception e) {
                    LOG.warn("close error", e);
                }
            }
            LOG.debug("close session end");
        }
    }
}
