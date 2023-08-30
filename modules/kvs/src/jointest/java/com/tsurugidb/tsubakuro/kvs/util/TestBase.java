package com.tsurugidb.tsubakuro.kvs.util;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;

public class TestBase {

    private static final String SYSPROP_KVSTEST_ENDPOINT = "tsurugi.kvstest.endpoint";
    private final URI endpoint;

    public TestBase() {
        String uri = System.getProperty(SYSPROP_KVSTEST_ENDPOINT, "ipc:tsurugi");
        this.endpoint = URI.create(uri);
        System.err.println("endpoint=" + endpoint);
    }

    public URI endpoint() {
        return endpoint;
    }

    public Session getNewSession() throws IOException, ServerException, InterruptedException {
        return SessionBuilder.connect(endpoint).withCredential(NullCredential.INSTANCE).create();
    }

}
