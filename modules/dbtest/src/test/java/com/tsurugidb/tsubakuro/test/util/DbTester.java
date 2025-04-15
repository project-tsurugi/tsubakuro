package com.tsurugidb.tsubakuro.test.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.debug.DebugClient;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

public class DbTester {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private static Session staticSession;
    private static ExecutorService staticService;
    private static DebugClient staticDebugClient;

    protected static Session getSession() throws IOException {
        synchronized (DbTester.class) {
            if (staticSession == null) {
                String baseLabel = DbTestConnector.getSessionLabel();
                String label = baseLabel + " (staticSession)";
                staticSession = DbTestConnector.createSession(label);
            }
        }
        return staticSession;
    }

    private static DebugClient getDebugClient() throws IOException, InterruptedException {
        synchronized (DbTester.class) {
            if (staticDebugClient == null) {
                var session = getSession();
                staticDebugClient = DebugClient.attach(session);
            }
        }
        return staticDebugClient;
    }

    protected static synchronized void closeStaticSession() throws IOException, InterruptedException, ServerException {
        try (var c1 = staticSession; var c2 = staticDebugClient) {
            // close only
        } finally {
            staticSession = null;
            staticDebugClient = null;
        }
    }

    @AfterAll
    static void testerAfterAll() throws IOException, InterruptedException, ServerException {
        try (var c1 = staticSession; var c2 = staticDebugClient; var c3 = (Closeable) () -> {
            if (staticService != null) {
                staticService.shutdownNow();
            }
        }) {
            // close only
        } finally {
            staticSession = null;
            staticDebugClient = null;
            staticService = null;
        }
    }

    private static final boolean START_END_LOG_INFO = true;

    protected static void logInitStart(Logger log, TestInfo info) {
        setSessionLabel(info, null, "init all");

        if (START_END_LOG_INFO) {
            log.info("init all start");
        } else {
            log.debug("init all start");
        }
        serverLog(log, null, "init all start");
    }

    protected static void logInitEnd(Logger log, TestInfo info) {
        setSessionLabel(info, null, "init all end");
        try {
            if (START_END_LOG_INFO) {
                log.info("init all end");
            } else {
                log.debug("init all end");
            }
            serverLog(log, null, "init all end");
        } finally {
            DbTestConnector.setSessionLabel(null);
        }
    }

    protected void logInitStart(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, "init");

        if (START_END_LOG_INFO) {
            LOG.info("{} init start", displayName);
        } else {
            LOG.debug("{} init start", displayName);
        }
        serverLog(LOG, displayName, "init start");
    }

    protected void logInitEnd(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, "init end");

        try {
            if (START_END_LOG_INFO) {
                LOG.info("{} init end", displayName);
            } else {
                LOG.debug("{} init end", displayName);
            }
            serverLog(LOG, displayName, "init end");
        } finally {
            DbTestConnector.setSessionLabel(null);
        }
    }

    @BeforeEach
    void tetsterBeforeEach(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, null);

        if (START_END_LOG_INFO) {
            LOG.info("{} start", displayName);
        } else {
            LOG.debug("{} start", displayName);
        }
        serverLog(LOG, displayName, "start");
    }

    @AfterEach
    void testerAfterEach(TestInfo info) {
        String displayName = getDisplayName(info);
        setSessionLabel(info, displayName, "end");

        try {
            if (START_END_LOG_INFO) {
                LOG.info("{} end", displayName);
            } else {
                LOG.debug("{} end", displayName);
            }
            serverLog(LOG, displayName, "end");
        } finally {
            DbTestConnector.setSessionLabel(null);
        }
    }

    private static void setSessionLabel(TestInfo info, String displayName, String suffix) {
        String className = info.getTestClass().map(c -> c.getSimpleName()).orElse("Unknown");
        String label = className;
        if (displayName != null) {
            label += "." + displayName;
        }
        if (suffix != null) {
            label += " " + suffix;
        }
        DbTestConnector.setSessionLabel(label);
    }

    private static String getDisplayName(TestInfo info) {
        String d = info.getDisplayName();
        String m = info.getTestMethod().map(Method::getName).orElse(null);
        if (m != null && !d.startsWith(m)) {
            return m + "() " + d;
        }
        return d;
    }

    protected static void serverLog(Logger log, String displayName, String message) {
        try {
            var client = getDebugClient();
            var m = "tsubakuro-dbtest: " + getServerLogName(log, displayName) + " " + message;
            client.logging(m).await(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("serverLog error. message={}", message, e);
        }
    }

    private static String getServerLogName(Logger log, String displayName) {
        String name = log.getName();
        int n = name.lastIndexOf('.');
        if (n >= 0) {
            name = name.substring(n + 1);
        }

        if (displayName == null) {
            return name;
        }
        return name + "." + displayName;
    }

    // property

    protected static String getSystemProperty(String key, String defaultValue) {
        String property = System.getProperty(key);
        return (property != null) ? property : defaultValue;
    }

    protected static int getSystemProperty(String key, int defaultValue) {
        String property = getSystemProperty(key, (String) null);
        return (property != null) ? Integer.parseInt(property) : defaultValue;
    }

    // utility

    protected static void dropTableIfExists(String tableName) throws IOException, ServerException, InterruptedException, TimeoutException {
        var sql = "drop table if exists " + tableName;
        executeDdl(sql);
    }

    protected static void executeDdl(String sql) throws IOException, ServerException, InterruptedException, TimeoutException {
        executeOcc(transaction -> {
            transaction.executeStatement(sql).await(10, TimeUnit.SECONDS);
        });
    }

    protected static interface TransactionAction {
        void execute(Transaction transaction) throws IOException, ServerException, InterruptedException, TimeoutException;
    }

    protected static void executeOcc(TransactionAction action) throws IOException, ServerException, InterruptedException, TimeoutException {
        executeOcc(action, null);
    }

    protected static interface ExceptionAction {
        void apply(Exception e) throws Exception;
    }

    protected static void executeOcc(TransactionAction action, ExceptionAction commitErrorAction) throws IOException, ServerException, InterruptedException, TimeoutException {
        try (var sqlClient = SqlClient.attach(getSession())) {
            var option = TransactionOption.newBuilder() //
                    .setType(TransactionType.SHORT) //
                    .setLabel("DbTester.executeOcc") //
                    .build();
            try (var transaction = sqlClient.createTransaction(option).await(10, TimeUnit.SECONDS)) {
                action.execute(transaction);
                try {
                    transaction.commit().await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (commitErrorAction != null) {
                        try {
                            commitErrorAction.apply(e);
                        } catch (IOException | ServerException | InterruptedException | TimeoutException e1) {
                            throw e1;
                        } catch (RuntimeException e1) {
                            throw e1;
                        } catch (Exception e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    @SafeVarargs
    protected static void close(Collection<? extends AutoCloseable>... list) throws Exception {
        Exception save = null;
        for (var closeableCollection : list) {
            if (closeableCollection == null) {
                continue;
            }
            for (var closeable : closeableCollection) {
                if (closeable == null) {
                    continue;
                }
                try {
                    if (closeable instanceof ServerResource) {
                        var resource = (ServerResource) closeable;
                        resource.setCloseTimeout(new Timeout(10, TimeUnit.SECONDS, Policy.ERROR));
                        resource.close();
                    } else {
                        closeable.close();
                    }
                } catch (Exception e) {
                    if (save == null) {
                        save = e;
                    } else {
                        save.addSuppressed(e);
                    }
                }
            }
        }

        if (save != null) {
            throw save;
        }
    }

    protected static void close(AutoCloseable... list) throws Exception {
        Exception save = null;
        for (var closeable : list) {
            try {
                if (closeable != null) {
                    if (closeable instanceof ServerResource) {
                        var resource = (ServerResource) closeable;
                        resource.setCloseTimeout(new Timeout(10, TimeUnit.SECONDS, Policy.ERROR));
                        resource.close();
                    } else {
                        closeable.close();
                    }
                }
            } catch (Exception e) {
                if (save == null) {
                    save = e;
                } else {
                    save.addSuppressed(e);
                }
            }
        }

        if (save != null) {
            throw save;
        }
    }

    // transaction option

    protected static SqlRequest.TransactionOption createTransactionOptionOcc(String label) {
        return SqlRequest.TransactionOption.newBuilder().setType(TransactionType.SHORT).setLabel(label).build();
    }
}
