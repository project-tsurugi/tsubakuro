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
    // processing status.
    enum Status {
        // initial state, Disposer thread is not running.
        INIT,

        // Disposer thread is running and accepting FutureResponses and Resources.
        OPEN,

        // shutdown has been initiated.
        SHUTDOWN,

        // session close has been initiated.
        CLOSED,
        ;

        String asString() {
            return name();
        }
    }

    static final Logger LOG = LoggerFactory.getLogger(Disposer.class);

    private ConcurrentLinkedQueue<ForegroundFutureResponse<?>> futureResponseQueue = new ConcurrentLinkedQueue<>();

    private ConcurrentLinkedQueue<DelayedClose> serverResourceQueue = new ConcurrentLinkedQueue<>();

    private ConcurrentLinkedQueue<DelayedShutdown> shutdownQueue = new ConcurrentLinkedQueue<>();

    private final AtomicReference<DelayedClose> close = new AtomicReference<>();

    private final Lock lock = new ReentrantLock();

    private final Condition empty = lock.newCondition();

    private AtomicReference<Status> status = new AtomicReference<>(Status.INIT);

    /**
     * Enclodure of delayed clean up procedure.
     */
    public interface DelayedShutdown {
        /**
         * clean up procedure.
         * @throws IOException An error was occurred while cleanUP() is executed.
         */
        void process() throws IOException;
    }

    /**
     * Enclodure of delayed clean up procedure.
     */
    public interface DelayedClose {
        /**
         * invoke the close() procedure of its belonging object.
         * @return true if close operation is completes
         * @throws ServerException if error was occurred while disposing this session
         * @throws IOException if I/O error was occurred while disposing this session
         * @throws InterruptedException if interrupted while disposing this session
         */
        boolean delayedClose() throws ServerException, IOException, InterruptedException;
    }

    /**
     * Creates a new instance.
     */
    public Disposer() {
    }

    @Override
    public void run() {
        Exception exception = null;

        while (true) {
            var futureResponse = futureResponseQueue.peek();
            if (futureResponse != null) {
                try {
                    var obj = futureResponse.retrieve();
                    if (obj instanceof ServerResource) {
                        ((ServerResource) obj).close();
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
                } finally {
                    futureResponseQueue.poll();
                }
                continue;
            }
            var serverResource = serverResourceQueue.peek();
            if (serverResource != null) {
                try {
                    if (!serverResource.delayedClose()) {
                        // The server response has not been received
                        serverResourceQueue.add(serverResource);
                        continue;
                    }
                } catch (ServerException | IOException | InterruptedException e) {
                    exception = addSuppressed(exception, e);
                } finally {
                    serverResourceQueue.poll();
                }
                continue;
            }
            boolean shoudContinue = false;
            lock.lock();
            try {
                if (!futureResponseQueue.isEmpty() || !serverResourceQueue.isEmpty()) {
                    continue;
                }
                empty.signalAll();
                shoudContinue = close.get() == null;
            } finally {
                lock.unlock();
            }
            if (shoudContinue) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // No problem, it's OK
                }
                continue;
            }
            status.set(Status.SHUTDOWN);
            while (!shutdownQueue.isEmpty()) {  // in case multiple shutdown requests are registered
                try {
                    shutdownQueue.poll().process();
                } catch (IOException e) {
                    exception = addSuppressed(exception, e);
                }
            }

            // confirm if we can go ahead to session close
            DelayedClose delayedClose = null;
            lock.lock();
            try {
                if (!futureResponseQueue.isEmpty() || !serverResourceQueue.isEmpty() || !shutdownQueue.isEmpty()) {
                    continue;
                }
                status.set(Status.CLOSED);
                delayedClose = close.get();
            } finally {
                lock.unlock();
            }

            if (delayedClose != null) {
                // go ahead to session close as futureResponseQueue, serverResourceQueue, and shutdownQueue are empty
                try {
                    delayedClose.delayedClose();
                } catch (ServerException | IOException | InterruptedException e) {
                    exception = addSuppressed(exception, e);
                }
                break;
            }

            // sleep outside of the lock
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // No problem, it's OK
            }
        }

        if (exception != null) {
            LOG.error(exception.getMessage(), exception);
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
            var currentStatus = status.get();
            if (currentStatus == Status.SHUTDOWN || currentStatus == Status.CLOSED) {
                throw new AssertionError("Disposer status: " + currentStatus.asString());
            }
            futureResponseQueue.add(futureResponse);
            if (status.get() == Status.INIT) {
                status.set(Status.OPEN);
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
            var currentStatus = status.get();
            if (currentStatus == Status.SHUTDOWN || currentStatus == Status.CLOSED) {
                throw new AssertionError("Disposer status: " + currentStatus.asString());
            }
            serverResourceQueue.add(resource);
            if (currentStatus == Status.INIT) {
                status.set(Status.OPEN);
                this.start();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Register a delayed shutdown procesure of the Session.
     * If disposer thread has not started, a disposer thread will be started.
     * NOTE: This method is assumed to be called only in close and/or shutdown of a Session.
     * @param cleanUp the clean up procesure to be registered
     * @throws IOException An error was occurred in c.shoutdown() execution.
     */
    public void registerDelayedShutdown(DelayedShutdown cleanUp) throws IOException {
        lock.lock();
        try {
            var currentStatus = status.get();
            if (currentStatus == Status.SHUTDOWN || currentStatus == Status.CLOSED) {
                throw new AssertionError("Disposer status: " + currentStatus.asString());
            }
            shutdownQueue.add(cleanUp);
            if (currentStatus == Status.INIT) {
                status.set(Status.OPEN);
                this.start();
            }
        } finally {
            lock.unlock();
        }
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
        waitForEmpty();
        lock.lock();
        try {
            var currentStatus = status.get();
            if (currentStatus == Status.CLOSED) {
                throw new AssertionError("Session close is already scheduled");
            } else if (currentStatus == Status.OPEN || currentStatus == Status.SHUTDOWN) {  // the same as `if daemon is runnint`
                close.set(cleanUp);
                return;
            }
            status.set(Status.CLOSED);
            cleanUp.delayedClose();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wait until the release of the server resource corresponding to the response
     * closed without getting is completed.
     * NOTE: This method must be called with the guarantee that no subsequent add() will be called.
     */
    public void waitForEmpty() {
        lock.lock();
        try {
            while (!futureResponseQueue.isEmpty() || !serverResourceQueue.isEmpty()) {
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
