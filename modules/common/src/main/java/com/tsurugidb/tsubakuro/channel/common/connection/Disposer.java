/*
 * Copyright 2023-2026 Project Tsurugi.
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
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * The disposer that disposes server resources corresponding to ForegroundFutureResponses that are closed without being gotten.
 */
public class Disposer extends Thread {
    // processing status.
    enum Status {
        // initial state, Disposer thread is not running.
        INACTIVE,

        // Disposer thread is running and accepting FutureResponses and Resources.
        HANDLE_ASYNC_CLOSE,

        // shutdown has been initiated.
        HANDLE_SESSION_SHUTDOWN,

        // session close has been initiated.
        HANDLE_SESSION_CLOSE,
        ;

        String asString() {
            return name();
        }
    }

    static final Logger LOG = LoggerFactory.getLogger(Disposer.class);

    private final ConcurrentLinkedQueue<ForegroundFutureResponse<?>> futureResponseQueue = new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<DelayedClose> serverResourceQueue = new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<DelayedShutdown> shutdownQueue = new ConcurrentLinkedQueue<>();

    private final AtomicReference<DelayedClose> sessionClose = new AtomicReference<>();

    private final Lock globalLock = new ReentrantLock();

    private static final class AtomicStatus extends ReentrantLock {
        private final AtomicReference<Status> status;
        private final Condition empty = newCondition();
        private final Condition entry = newCondition();

        AtomicStatus(Status initialStatus) {
            this.status = new AtomicReference<>(initialStatus);
        }

        Status get() {
            if (getOwner() != Thread.currentThread()) {
                throw new IllegalStateException("lock must be held when get() is called");
            }
           return status.get();
        }

        void set(Status newStatus) {
            if (getOwner() != Thread.currentThread()) {
                throw new IllegalStateException("lock must be held when set() is called");
            }
            status.set(newStatus);
        }

        Condition emptyCondition() {
            return empty;
        }

        Condition entryCondition() {
            return entry;
        }
    }

    private final AtomicStatus status = new AtomicStatus(Status.INACTIVE);

    private static final long PATROL_CYCLE_TIME_NANOS = 1_000_000_000L;  // 1 second

    /**
     * Enclosure of delayed clean up procedure.
     */
    public interface DelayedShutdown {
        /**
         * clean up procedure.
         * @throws IOException An error was occurred while cleanUP() is executed.
         */
        void process() throws IOException;
    }

    /**
     * Enclosure of delayed clean up procedure.
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
        globalLock.lock();  // ensure single run at a time
        try {
            bodyRun();
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * Delayedclean up procedure body.
     */
    private void bodyRun() {
        Exception exception = null;

        while (true) {
            var futureResponse = futureResponseQueue.peek();
            if (futureResponse != null) {
                boolean doAdd = false;
                try {
                    var obj = futureResponse.cleanUp();
                    if (obj instanceof ServerResource) {
                        ((ServerResource) obj).close();
                    }
                } catch (ChannelResponse.AlreadyCanceledException | ForegroundFutureResponse.AlreadyClosedException | SessionAlreadyClosedException e) {
                    // Server resource has not created at the server side, or session is already closed
                } catch (TimeoutException e) {
                    // Let's try again, as server resource has been disposed by the session close
                    doAdd = true;
                } catch (ServerException | IOException | InterruptedException e) {
                    boolean ignore = false;
                    if (e instanceof CoreServiceException) {
                        // ignore OPERATION_CANCELED error
                        if (((CoreServiceException) e).getDiagnosticCode() == CoreServiceCode.OPERATION_CANCELED) {
                            ignore = true;
                        }
                    }
                    if (!ignore) {
                        // should not occur
                        if (exception == null) {
                            exception = e;
                        } else {
                            exception.addSuppressed(e);
                        }
                    }
                } finally {
                    futureResponseQueue.poll();
                    if (doAdd) {
                        futureResponseQueue.add(futureResponse);
                    }
                }
                continue;
            }

            var serverResource = serverResourceQueue.peek();
            if (serverResource != null) {
                boolean doAdd = false;
                try {
                    if (!serverResource.delayedClose()) {
                        // The server response has not been received
                        doAdd = true;
                    }
                } catch (ServerException | IOException | InterruptedException e) {
                    exception = addSuppressed(exception, e);
                } finally {
                    serverResourceQueue.poll();
                    if (doAdd) {
                        serverResourceQueue.add(serverResource);
                    }
                }
                continue;
            }

            boolean shouldContinue = false;
            status.lock();
            try {
                if (futureResponseQueue.isEmpty() && serverResourceQueue.isEmpty()) {
                    status.emptyCondition().signalAll();
                    shouldContinue = shutdownQueue.isEmpty() && sessionClose.get() == null;
                    if (shouldContinue) {
                        try {
                            status.entryCondition().awaitNanos(PATROL_CYCLE_TIME_NANOS);
                        } catch (InterruptedException e) {
                            // No problem, it's OK
                        }
                    } else {
                        status.set(Status.HANDLE_SESSION_SHUTDOWN);
                    }
                } else {
                    shouldContinue = true;
                }
            } finally {
                status.unlock();
            }
            if (shouldContinue) {
                continue;
            }

            while (true) {
                while (!shutdownQueue.isEmpty()) {  // in case multiple shutdown requests are registered
                    try {
                        shutdownQueue.poll().process();
                    } catch (IOException e) {
                        exception = addSuppressed(exception, e);
                    }
                }

                // confirm if we can go ahead to session close
                shouldContinue = (sessionClose.get() == null);
                if (shouldContinue) {
                    status.lock();
                    try {
                        try {
                            status.entryCondition().awaitNanos(PATROL_CYCLE_TIME_NANOS);
                        } catch (InterruptedException e) {
                            // No problem, it's OK
                        }
                    } finally {
                        status.unlock();
                    }
                }
                if (shouldContinue) {
                    continue;
                }

                status.lock();
                try {
                    status.set(Status.HANDLE_SESSION_CLOSE);
                } finally {
                    status.unlock();
                }
                try {
                    sessionClose.get().delayedClose();
                } catch (ServerException | IOException | InterruptedException e) {
                    exception = addSuppressed(exception, e);
                }
                break;
            }
            break;
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
        status.lock();
        try {
            var currentStatus = status.get();
            if (currentStatus == Status.HANDLE_SESSION_SHUTDOWN || currentStatus == Status.HANDLE_SESSION_CLOSE) {
                throw new AssertionError("Disposer status: " + currentStatus.asString());
            }
            futureResponseQueue.add(futureResponse);
            if (currentStatus == Status.INACTIVE) {
                status.set(Status.HANDLE_ASYNC_CLOSE);
                this.setDaemon(true);
                this.start();
            } else {
                status.entryCondition().signalAll();
            }
        } finally {
            status.unlock();
        }
    }

    /**
     * Add a DelayedClose object containing a close procedure for a certain ServerResource object.
     * If disposer thread has not started, a disposer thread will be started.
     * @param resource the DelayedClose to be added
     */
    public void add(DelayedClose resource) {
        status.lock();
        try {
            var currentStatus = status.get();
            if (currentStatus == Status.HANDLE_SESSION_SHUTDOWN || currentStatus == Status.HANDLE_SESSION_CLOSE) {
                throw new AssertionError("Disposer status: " + currentStatus.asString());
            }
            serverResourceQueue.add(resource);
            if (currentStatus == Status.INACTIVE) {
                status.set(Status.HANDLE_ASYNC_CLOSE);
                this.setDaemon(true);
                this.start();
            } else {
                status.entryCondition().signalAll();
            }
        } finally {
            status.unlock();
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
        status.lock();
        try {
            var currentStatus = status.get();
            if (currentStatus == Status.HANDLE_SESSION_CLOSE) {
                throw new AssertionError("Disposer status: " + currentStatus.asString());
            }
            shutdownQueue.add(cleanUp);
            if (currentStatus == Status.INACTIVE) {
                status.set(Status.HANDLE_ASYNC_CLOSE);
                this.setDaemon(true);
                this.start();
            } else {
                status.entryCondition().signalAll();
            }
        } finally {
            status.unlock();
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
        status.lock();
        try {
            var currentStatus = status.get();
            if (currentStatus == Status.HANDLE_SESSION_CLOSE) {
                throw new AssertionError("Session close is already scheduled");
            } else if (currentStatus == Status.HANDLE_ASYNC_CLOSE || currentStatus == Status.HANDLE_SESSION_SHUTDOWN) {  // the same as `if daemon is running`
                sessionClose.set(cleanUp);
                status.entryCondition().signalAll();
                return;
            }
            status.set(Status.HANDLE_SESSION_CLOSE);
        } finally {
            status.unlock();
        }
        cleanUp.delayedClose();  // execute outside the lock
    }

    /**
     * Wait until the release of the server resource corresponding to the response
     * closed without getting is completed.
     * NOTE: This method must be called with the guarantee that no subsequent add() will be called.
     */
    public void waitForEmpty() {
        status.lock();
        try {
            while (!futureResponseQueue.isEmpty() || !serverResourceQueue.isEmpty()) {
                try {
                    status.emptyCondition().awaitNanos(PATROL_CYCLE_TIME_NANOS);
                } catch (InterruptedException e) {
                    // No problem, it's OK
                }
            }
        } finally {
            status.unlock();
        }
    }
}
