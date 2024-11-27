package com.tsurugidb.tsubakuro.test.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class DbTestConnector {

    private static final String SYSPROP_DBTEST_ENDPOINT = "tsurugi.dbtest.endpoint";

    private static URI staticEndpoint;

    private static String sessionLabel;
    private static long defaultTimeoutValue = 10;
    private static TimeUnit defaultTimeoutUnit = TimeUnit.SECONDS;

    public static URI getEndPoint() {
        if (staticEndpoint == null) {
            String endpoint = System.getProperty(SYSPROP_DBTEST_ENDPOINT, "tcp://localhost:12345");
            staticEndpoint = URI.create(endpoint);
        }
        return staticEndpoint;
    }

    public static boolean isIpc() {
        URI endpoint = getEndPoint();
        String scheme = endpoint.getScheme();
        return scheme.equals("ipc");
    }

    public static boolean isTcp() {
        URI endpoint = getEndPoint();
        String scheme = endpoint.getScheme();
        return scheme.equals("tcp");
    }

    public static void setSessionLabel(String label) {
        sessionLabel = label;
    }

    public static String getSessionLabel() {
        return sessionLabel;
    }

    public static FutureResponse<? extends Session> createSessionFuture(String label) {
        FutureResponse<? extends Session> future;
        {
            var builder = createSessionBuilder(label);
            try {
                future = builder.createAsync();
            } catch (RuntimeException e) {
                throw e;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return future;
    }

    public static Session createSession() {
        return createSession(sessionLabel);
    }

    public static Session createSession(String label) {
        try {
            return createSession0(label);
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Session createSession0(String label) throws IOException, ServerException, InterruptedException, TimeoutException {
        var builder = createSessionBuilder(label);
        return builder.create(defaultTimeoutValue, defaultTimeoutUnit);
    }

    public static SessionBuilder createSessionBuilder() {
        return createSessionBuilder(sessionLabel);
    }

    public static SessionBuilder createSessionBuilder(String label) {
        var endpoint = getEndPoint();
        var builder = SessionBuilder.connect(endpoint) //
                .withApplicationName("tsubakuro-dbtest") //
                .withLabel(label);
        return builder;
    }
}
