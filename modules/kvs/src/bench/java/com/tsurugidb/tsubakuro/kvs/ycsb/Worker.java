package com.tsurugidb.tsubakuro.kvs.ycsb;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;

import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * worker thread
 */
public abstract class Worker implements Callable<Long> {

    /**
     * manager for all clients to start/quit
     */
    protected final RunManager mgr;

    /**
     * endpoint such as "ipc:tsubakuro"
     */
    protected final URI endpoint;

    /**
     * whether create database before benchmark
     */
    protected final boolean createDB;

    /**
     * table name used by this worker
     */
    protected final String tableName;

    /**
     * number of clients
     */
    protected final int numClient;

    /**
     * id of client
     */
    protected final int clientId;

    /**
     * read operation (i.e. GET) ratio: 0..100
     */
    protected final int rratio;

    /**
     * running time of benchmark in milli seconds
     */
    protected final long runMsec;

    /**
     * operation list
     */
    protected final ArrayList<Operation> operations = new ArrayList<>(Constants.NUM_RECORDS);

    Worker(RunManager mgr, URI endpoint, boolean createDB, int numClient, int clientId, int rratio, long runMsec) throws Exception {
        this.mgr = mgr;
        this.endpoint = endpoint;
        if (Constants.USE_SAME_TABLE) {
            this.createDB = clientId == 0 ? createDB : false;
            this.tableName = Constants.TABLE_NAME;
        } else {
            this.createDB = createDB;
            this.tableName = Constants.TABLE_NAME + clientId;
        }
        this.numClient = numClient;
        this.clientId = clientId;
        this.rratio = rratio;
        this.runMsec = runMsec;
    }

    /**
     * initialize database before benchmark
     * @throws Exception if failed
     */
    protected void initDB() throws Exception {
        mgr.addReadyWorker();
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

    /**
     * create operation list including PUT and GET randomly
     */
    protected void createOperations() {
        operations.clear();
        for (int i = 0; i < Constants.NUM_RECORDS; i++) {
            boolean isGet = 100 * Math.random() < rratio;
            long key = (i * numClient + clientId) % Constants.NUM_RECORDS;
            operations.add(new Operation(isGet, key));
        }
        Collections.shuffle(operations);
    }

    @Override
    public Long call() throws Exception {
        // System.err.println("start client Thread for " + tableName);
        if (createDB) {
            initDB();
            return Long.valueOf(0L);
        }
        if (runMsec <= 0) {
            return Long.valueOf(0L);
        }
        createOperations();
        return benchmark();
    }

    /**
     * @return number of executed transactions
     * @throws Exception if failed
     */
    protected abstract Long benchmark() throws Exception;

}
