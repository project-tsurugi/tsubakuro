package com.tsurugidb.tsubakuro.kvs.ycsb;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;

import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * worker thread
 */
public class Worker implements Callable<Long> {

    private final URI endpoint;
    private final boolean createDB;
    private final String tableName;
    private final int rratio;
    private final long runMsec;
    private final ArrayList<Operation> operations = new ArrayList<>();

    Worker(URI endpoint, boolean createDB, int clientId, int rratio, long runMsec) throws Exception {
        this.endpoint = endpoint;
        this.createDB = createDB;
        this.tableName = Constants.TABLE_NAME + clientId;
        this.rratio = rratio;
        this.runMsec = runMsec;
    }

    private void initDB() throws Exception {
        try (var session = SessionBuilder.connect(endpoint).withCredential(NullCredential.INSTANCE).create();
            var client = SqlClient.attach(session);) {
            try (var tx = client.createTransaction().await()) {
                String sql = String.format("CREATE TABLE %s (%s %s PRIMARY KEY, %s %s)", tableName, Constants.KEY_NAME,
                        Constants.KEY_TYPE, Constants.VALUE_NAME, Constants.VALUE_TYPE);
                tx.executeStatement(sql).await();
                tx.commit().await();
                // System.err.println(sql);
                System.out.println("table " + tableName + " created");
            }
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

    private void createOperations() {
        operations.clear();
        for (int i = 0; i < Constants.NUM_RECORDS; i++) {
            boolean isGet = 100 * Math.random() < rratio;
            operations.add(new Operation(isGet, i));
        }
        Collections.shuffle(operations);
    }

    @Override
    public Long call() throws Exception {
        // System.err.println("start client Thread for " + tableName);
        if (createDB) {
            initDB();
        }
        if (runMsec <= 0) {
            return Long.valueOf(0L);
        }
        createOperations();
        int optId = 0;
        long numTx = 0;
        try (var session = SessionBuilder.connect(endpoint).withCredential(NullCredential.INSTANCE).create();
            var kvs = KvsClient.attach(session)) {
            long start = System.currentTimeMillis();
            do {
                optId = 0;
                while (optId < operations.size()) {
                    try (var tx = kvs.beginTransaction().await()) {
                        for (int i = 0; i < Constants.OPS_PER_TX; i++, optId++) {
                            var op = operations.get(optId % operations.size());
                            RecordBuffer buffer = new RecordBuffer();
                            buffer.add(Constants.KEY_NAME, Long.valueOf(op.key()));
                            if (op.isGet()) {
                                kvs.get(tx, tableName, buffer).await();
                            } else {
                                buffer.add(Constants.VALUE_NAME, Long.valueOf(100L * i));
                                kvs.put(tx, tableName, buffer, PutType.IF_PRESENT).await();
                            }
                        }
                        kvs.commit(tx).await();
                    }
                    numTx++;
                }
            } while (System.currentTimeMillis() - start < runMsec);
        }
        // System.err.println("finish client Thread for " + tableName + ", numTx=" + numTx);
        return Long.valueOf(numTx);
    }

}
