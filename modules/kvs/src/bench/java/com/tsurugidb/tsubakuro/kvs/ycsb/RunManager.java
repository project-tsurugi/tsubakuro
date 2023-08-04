package com.tsurugidb.tsubakuro.kvs.ycsb;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * the manager for all workers to judge to be started/quit
 */
public class RunManager {

    private final int numClient;

    private final AtomicInteger numReady = new AtomicInteger(0);

    private final AtomicBoolean bStart = new AtomicBoolean(false);

    private final AtomicBoolean bQuit = new AtomicBoolean(false);

    RunManager(int numClient) {
        this.numClient = numClient;
    }

    /**
     * register one new client is ready to start
     */
    public void addReadyWorker() {
        numReady.incrementAndGet();
        System.err.println("numReady=" + numReady.get() + ", numClient=" + numClient);
    }

    /**
     * wait until all workers are ready to start
     */
    public void waitUntilAllWorkresReady() {
        while (numReady.get() < numClient) {
            ;
        }
        System.err.println("set bStart = true");
        bStart.set(true);
    }

    /**
     * retrieves all workers are ready to start or not
     * @return whether all workers are ready to start or not
     */
    public boolean isAllWorkersReady() {
        return bStart.get();
    }

    /**
     * set internal state to quit all workers
     */
    public void setQuit() {
        System.err.println("set bQuit = true");
        bQuit.set(true);
    }

    /**
     * retrieves all workers should be quit
     * @return whether all workers should be quit
     */
    public boolean isQuit() {
        return bQuit.get();
    }

}
