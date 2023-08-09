package com.tsurugidb.tsubakuro.kvs.ycsb;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;

import com.tsurugidb.tsubakuro.kvs.util.RunManager;

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
     * operation list
     */
    protected final ArrayList<Operation> operations = new ArrayList<>(Constants.NUM_RECORDS);

    Worker(RunManager mgr, URI endpoint, int numClient, int clientId, int rratio) throws Exception {
        this.mgr = mgr;
        this.endpoint = endpoint;
        if (Constants.USE_SAME_TABLE) {
            this.tableName = Constants.TABLE_NAME;
        } else {
            this.tableName = Constants.TABLE_NAME + clientId;
        }
        this.numClient = numClient;
        this.clientId = clientId;
        this.rratio = rratio;
    }

    Worker(RunManager mgr, URI endpoint, int clientId) throws Exception {
        this(mgr, endpoint, 0, clientId, 0);
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
        createOperations();
        return benchmark();
    }

    /**
     * @return number of executed transactions
     * @throws Exception if failed
     */
    protected abstract Long benchmark() throws Exception;

}
