package com.tsurugidb.tsubakuro.kvs.util;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;

public class TestBase {

    private static final String SYSPROP_KVSTEST_ENDPOINT = "tsurugi.kvstest.endpoint";
    private static final URI ENDPOINT;

    static {
        String uri = System.getProperty(SYSPROP_KVSTEST_ENDPOINT, "ipc:tsurugi");
        ENDPOINT = URI.create(uri);
        System.err.println("endpoint=" + ENDPOINT);
    }

    public Session getNewSession() throws IOException, ServerException, InterruptedException {
        return SessionBuilder.connect(ENDPOINT).withCredential(NullCredential.INSTANCE).create();
    }

    private void dropTable(SqlClient client, String tableName) throws Exception {
        try {
            try (var tx = client.createTransaction().await()) {
                String sql = String.format("DROP TABLE %s", tableName);
                tx.executeStatement(sql).await();
                tx.commit().await();
            }
        } catch (Exception e) {
            var msg = e.getMessage();
            if (!msg.contains("table_not_found") && !msg.contains("not found")) {
                throw e;
            }
        }
    }

    public void dropTable(String tableName) throws Exception {
        try (var session = getNewSession(); var client = SqlClient.attach(session)) {
            dropTable(client, tableName);
        }
    }

    public void createTable(String tableName, String schema) throws Exception {
        try (var session = getNewSession(); var client = SqlClient.attach(session)) {
            dropTable(client, tableName);
            try (var tx = client.createTransaction().await()) {
                String sql = String.format("CREATE TABLE %s (%s)", tableName, schema);
                tx.executeStatement(sql).await();
                tx.commit().await();
            }
        }
    }

}
