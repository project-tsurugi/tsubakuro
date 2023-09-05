package com.tsurugidb.tsubakuro.kvs.util;

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

    /**
     * Creates a new instance. (for a main thread)
     * @param numClient number of all workers
     */
    public RunManager(int numClient) {
        this.numClient = numClient;
    }

    /**
     * register one new worker is ready to start (for a worker thread)
     */
    public void addReadyWorker() {
        numReady.incrementAndGet();
    }

    /**
     * set start after waiting all workers are ready (for a main thread)
     */
    @SuppressWarnings("EmptyBlock")
    public void setWorkerStartTime() {
        while (numReady.get() < numClient) {
            // finite loop
        }
        bStart.set(true);
    }

    /**
     * wait until all workers are ready to start (for a worker thread)
     */
    @SuppressWarnings("EmptyBlock")
    public void waitUntilWorkerStartTime() {
        while (!bStart.get()) {
            // finite loop
        }
    }

    /**
     * set internal state to quit all workers (for a main thread)
     */
    public void setQuit() {
        bQuit.set(true);
    }

    /**
     * retrieves a worker should be quit (for a worker thread)
     * @return whether a worker should be quit
     */
    public boolean isQuit() {
        return bQuit.get();
    }

}
