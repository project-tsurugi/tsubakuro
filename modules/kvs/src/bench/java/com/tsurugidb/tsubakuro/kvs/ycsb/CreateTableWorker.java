package com.tsurugidb.tsubakuro.kvs.ycsb;

import java.net.URI;
import java.util.concurrent.Callable;

import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.SqlClient;

class CreateTableWorker implements Callable<Void> {

    private final URI endpoint;
    private final String tableName;

    CreateTableWorker(URI endpoint, int clientId) throws Exception {
        this.endpoint = endpoint;
        if (Constants.USE_SAME_TABLE) {
            this.tableName = Constants.TABLE_NAME;
        } else {
            this.tableName = Constants.TABLE_NAME + clientId;
        }
    }

    private void createTable() throws Exception {
        try (var session = SessionBuilder.connect(endpoint).withCredential(NullCredential.INSTANCE).create();
            var client = SqlClient.attach(session)) {
            try (var tx = client.createTransaction().await()) {
                String sql = String.format("CREATE TABLE %s (%s %s PRIMARY KEY, %s %s)", tableName, Constants.KEY_NAME,
                        Constants.KEY_TYPE, Constants.VALUE_NAME, Constants.VALUE_TYPE);
                tx.executeStatement(sql).await();
                tx.commit().await();
                System.out.println("table " + tableName + " created");
            }
        }
    }

    void putRecords() throws Exception {
        long key = 0;
        try (var session = SessionBuilder.connect(endpoint).withCredential(NullCredential.INSTANCE).create();
            var kvs = KvsClient.attach(session)) {
            while (key < Constants.NUM_RECORDS) {
                try (var tx = kvs.beginTransaction().await()) {
                    RecordBuffer buffer = new RecordBuffer();
                    for (int i = 0; i < Constants.OPS_PER_TX; i++, key++) {
                        buffer.add(Constants.KEY_NAME, Long.valueOf(key));
                        buffer.add(Constants.VALUE_NAME, Long.valueOf(0));
                        kvs.put(tx, tableName, buffer, PutType.OVERWRITE).await();
                    }
                    kvs.commit(tx).await();
                }
            }
        }
        System.out.println(key + " records inserted to " + tableName);
    }

    void insertRecords() throws Exception {
        try (var session = SessionBuilder.connect(endpoint).withCredential(NullCredential.INSTANCE).create();
            var client = SqlClient.attach(session);) {
            int key = 0;
            while (key < Constants.NUM_RECORDS) {
                try (var tx = client.createTransaction().await()) {
                    for (int k = 0; k < Constants.OPS_PER_TX; k++, key++) {
                        String sql = String.format("INSERT INTO %s (%s, %s) VALUES(%d, %d)", tableName,
                                Constants.KEY_NAME, Constants.VALUE_NAME, key, 0);
                        tx.executeStatement(sql);
                        // System.err.println(sql);
                    }
                    tx.commit();
                    // System.err.println("commit");
                }
            }
            System.out.println(Constants.NUM_RECORDS + " records inserted to " + tableName);
        }
    }
    @Override
    public Void call() throws Exception {
        createTable();
        // putRecords();
        insertRecords();
        return null;
    }

}
