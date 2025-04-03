/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.client.SessionAlreadyClosedException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * The disposer that disposes server resources corresponding to ForegroundFutureResponses that are closed without being gotten.
 */
public class Disposer extends Thread {
    static final Logger LOG = LoggerFactory.getLogger(Disposer.class);

    private AtomicBoolean started = new AtomicBoolean();

    private ConcurrentLinkedQueue<ForegroundFutureResponse<?>> futureResponseQueue = new ConcurrentLinkedQueue<>();

    private ConcurrentLinkedQueue<DelayedClose> serverResourceQueue = new ConcurrentLinkedQueue<>();

    private ConcurrentLinkedQueue<DelayedShutdown> shutdownQueue = new ConcurrentLinkedQueue<>();

    private ServerResource closingNow = null;

    private final AtomicReference<DelayedClose> close = new AtomicReference<>();

    private AtomicBoolean working = new AtomicBoolean();

    private final Lock lock = new ReentrantLock();

    private final Condition empty = lock.newCondition();

    /**
     * Enclodure of delayed clean up procedure.
     */
    public interface DelayedShutdown {
        /**
         * clean up procedure.
         * @throws IOException An error was occurred while cleanUP() is executed.
         */
        void shutdown() throws IOException;
    }

    /**
     * Enclodure of delayed clean up procedure.
     */
    public interface DelayedClose {
        /**
         * invoke the close() procedure of its belonging object.
         * @throws ServerException if error was occurred while disposing this session
         * @throws IOException if I/O error was occurred while disposing this session
         * @throws InterruptedException if interrupted while disposing this session
         */
        void delayedClose() throws ServerException, IOException, InterruptedException;
    }

    /**
     * Creates a new instance.
     */
    public Disposer() {
    }

    @Override
    public void run() {
        Exception exception = null;
        boolean shutdownProcessed = false;

        while (true) {
            working.set(true);
            var futureResponse = futureResponseQueue.poll();
            if (futureResponse != null) {
                try {
                    var obj = futureResponse.retrieve();
                    if (obj instanceof ServerResource) {
                        closingNow = (ServerResource) obj;
                        ((ServerResource) obj).close();
                        closingNow = null;
                    }
                } catch (ChannelResponse.AlreadyCanceledException e) {
                    // Server resource has not created at the server
                } catch (SessionAlreadyClosedException e) {
                    // Server resource has been disposed by the session close
                } catch (TimeoutException e) {
                    // Let's try again
                    futureResponseQueue.add(futureResponse);
                } catch (Exception e) {     // should not occur
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                }
                continue;
            }
            var serverResource = serverResourceQueue.poll();
            if (serverResource != null) {
                try {
                    serverResource.delayedClose();
                } catch (ServerException | IOException | InterruptedException e) {
                    exception = addSuppressed(exception, e);
                }
                continue;
            }
            lock.lock();
            try {
                working.set(false);
                empty.signalAll();
            } finally {
                lock.unlock();
            }
            if (!shutdownProcessed) {
                try {
                    while (!shutdownQueue.isEmpty()) {
                        shutdownQueue.poll().shutdown();
                    }
                    shutdownProcessed = true;
                } catch (IOException e) {
                    exception = addSuppressed(exception, e);
                }
            }
            var cl = close.get();
            if (cl != null) {
                if (shutdownProcessed || shutdownQueue.isEmpty()) {
                    try {
                        cl.delayedClose();
                    } catch (ServerException | IOException | InterruptedException e) {
                        exception = addSuppressed(exception, e);
                    }
                    break;
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // No problem, it's OK
            }
        }

        if (exception != null) {
            LOG.error(exception.getMessage());
            exception.printStackTrace();
            if (exception instanceof IOException) {
                throw new UncheckedIOException((IOException) exception);
            } else {
                throw new UncheckedIOException(new IOException(exception));
            }
        }
    }

    private Exception addSuppressed(Exception exception, Exception e) {
        if (exception == null) {
            exception = e;
        } else {
            exception.addSuppressed(e);
        }
        return exception;
    }

    void add(ForegroundFutureResponse<?> futureResponse) {
        lock.lock();
        try {
            if (close.get() != null || !shutdownQueue.isEmpty()) {
                throw new AssertionError("Session already closed");
            }
            futureResponseQueue.add(futureResponse);
            if (!started.getAndSet(true)) {
                this.start();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Add a DelayedClose object containing a close procedure for a certain ServerResource object.
     * If disposer thread has not started, a disposer thread will be started.
     * @param resource the DelayedClose to be added
     */
    public void add(DelayedClose resource) {
        lock.lock();
        try {
            if (close.get() != null || !shutdownQueue.isEmpty()) {
                throw new AssertionError("Session already closed");
            }
            serverResourceQueue.add(resource);
            if (!started.getAndSet(true)) {
                this.start();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Register a delayed shutdown procesure of the Session.
     * If disposer thread has not started, cleanUp.shoutdown() is immediately executed.
     * NOTE: This method is assumed to be called only in close and/or shutdown of a Session.
     * @param cleanUp the clean up procesure to be registered
     * @throws IOException An error was occurred in c.shoutdown() execution.
     */
    public void registerDelayedShutdown(DelayedShutdown cleanUp) throws IOException {
        lock.lock();
        try {
            if (close.get() != null) {
                throw new AssertionError("Session already closed");
            }
            if (started.getAndSet(true)) {  // true if daemon is working
                if (!futureResponseQueue.isEmpty() || !serverResourceQueue.isEmpty()) {
                    shutdownQueue.add(cleanUp);
                    return;
                }
                shutdownQueue.add(new DelayedShutdown() {
                    @Override
                    public void shutdown() {
                        // do nothing
                    }
                });
            }
            empty.signalAll();
        } finally {
            lock.unlock();
        }
        cleanUp.shutdown();
    }

    /**
     * Register a delayed close object in charge of asynchronous close of the Session.
     * If disposer thread has not started or both queue is empty, c.delayedClose() is immediately executed.
     * NOTE: This method is assumed to be called only in close and/or shutdown of a Session.
     * @param cleanUp the clean up procesure to be registered
     * @throws ServerException if server error was occurred while disposing the session
     * @throws IOException if I/O error was occurred while disposing the session
     * @throws InterruptedException if interrupted while disposing the session
     */
    public void registerDelayedClose(DelayedClose cleanUp) throws ServerException, IOException, InterruptedException {
        lock.lock();
        try {
            if (started.getAndSet(true)) {  // true if daemon is working
                if (!futureResponseQueue.isEmpty() || !serverResourceQueue.isEmpty()) {
                    close.set(cleanUp);
                    return;
                }
                close.set(new DelayedClose() {
                    @Override
                    public void  delayedClose() {
                        // do nothing
                    }
                });
            }
            empty.signalAll();
        } finally {
            lock.unlock();
        }
        cleanUp.delayedClose();
    }

    /**
     * Method to check if the disposer is calling close on the ServerResource given.
     * This method is provided in order to deal with both the case where a FutureResponse is closed without get
     * and the case where close is taken place for a ServerResource that has gotton from a FutureResponse.
     * ServerResource.close() should implement in that it performs close processing when it has been called from the Disposer,
     * and it registers a delayed close with the Disposer when it has been called from a non-disposer module.
     * @param serverResource ServerResource to be queried whether it is closed from the Disposer or not
     * @return true if serverResource.close() is currently being called by this disposer
     */
    public boolean isClosingNow(ServerResource serverResource) {
        return serverResource == closingNow;
    }

    /**
     * Wait until the release of the server resource corresponding to the response
     * closed without getting is completed.
     * NOTE: This method must be called with the guarantee that no subsequent add() will be called.
     */
    public void waitForEmpty() {
        lock.lock();
        try {
            while (!futureResponseQueue.isEmpty() || !serverResourceQueue.isEmpty() || working.get()) {
                try {
                    empty.await();
                } catch (InterruptedException e) {
                    continue;
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
