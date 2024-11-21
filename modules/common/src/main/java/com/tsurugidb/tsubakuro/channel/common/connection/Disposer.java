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
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    private Queue<ForegroundFutureResponse<?>> futureResponseQueue = new ArrayDeque<>();

    private Queue<DelayedClose> serverResourceQueue = new ArrayDeque<>();

    private AtomicBoolean empty = new AtomicBoolean();

    private final AtomicReference<DelayedShutdown> shutdown = new AtomicReference<>();

    private final AtomicReference<DelayedClose> close = new AtomicReference<>();

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

        while (true) {
            ForegroundFutureResponse<?> futureResponse;
            synchronized (futureResponseQueue) {
                futureResponse = futureResponseQueue.poll();
            }
            if (futureResponse != null) {
                try {
                    var obj = futureResponse.retrieve();
                    if (obj instanceof ServerResource) {
                        ((ServerResource) obj).close();
                    }
                    continue;
                } catch (ChannelResponse.AlreadyCanceledException e) {
                    // Server resource has not created at the server
                    continue;
                } catch (SessionAlreadyClosedException e) {
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                    continue;
                } catch (TimeoutException e) {
                    // Let's try again
                    futureResponseQueue.add(futureResponse);
                    continue;
                } catch (Exception e) {     // should not occur
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                    continue;
                }
            }
            DelayedClose serverResource;
            synchronized (serverResourceQueue) {
                serverResource = serverResourceQueue.poll();
            }
            if (serverResource != null) {
                try {
                    serverResource.delayedClose();
                } catch (ServerException | IOException | InterruptedException e) {
                    exception = addSuppressed(exception, e);
                }
                continue;
            }
            notifyEmpty();
            if (shutdown.get() != null || close.get() != null) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // No problem, it's OK
            }
        }

        try {
            var sh = shutdown.get();
            if (sh != null) {
                sh.shutdown();
            }
        } catch (IOException e) {
            exception = addSuppressed(exception, e);
        } finally {
            try {
                var cl = close.get();
                if (cl != null) {
                    cl.delayedClose();
                }
            } catch (ServerException | IOException | InterruptedException e) {
                exception = addSuppressed(exception, e);
            }
        }

        if (exception != null) {
            LOG.info(exception.getMessage());
            throw new UncheckedIOException(new IOException(exception));
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

    synchronized void add(ForegroundFutureResponse<?> futureResponse) {
        if (shutdown.get() != null || close.get() != null) {
            throw new AssertionError("Session already closed");
        }
        synchronized (futureResponseQueue) {
            futureResponseQueue.add(futureResponse);
        }
        if (!started.getAndSet(true)) {
            this.start();
        }
    }

    /**
     * Add a DelayedClose object containing a close procedure for a certain ServerResource object.
     * If disposer thread has not started, a disposer thread will be started.
     * @param resource the DelayedClose to be added
     */
    public synchronized void add(DelayedClose resource) {
        if (shutdown.get() != null || close.get() != null) {
            throw new AssertionError("Session already closed");
        }
        synchronized (serverResourceQueue) {
            serverResourceQueue.add(resource);
        }
        if (!started.getAndSet(true)) {
            this.start();
        }
    }

    /**
     * Register a delayed shutdown procesure of the Session.
     * If disposer thread has not started, cleanUp() is executed.
     * NOTE: This method is assumed to be called only in close and/or shutdown of a Session.
     * @param c the clean up procesure to be registered
     * @throws IOException An error was occurred when c.shoutdown() has been immediately executed.
     */
    public synchronized void registerDelayedShutdown(DelayedShutdown c) throws IOException {
        if (!started.getAndSet(true)) {
            c.shutdown();
            empty.set(true);
            return;
        }
        shutdown.set(c);
    }

    /**
     * Register a delayed close object in charge of asynchronous close of the Session.
     * If disposer thread has not started, cleanUp() is executed.
     * NOTE: This method is assumed to be called only in close and/or shutdown of a Session.
     * @param c the clean up procesure to be registered
     * @throws ServerException if error was occurred while disposing this session
     * @throws IOException if I/O error was occurred while disposing this session
     * @throws InterruptedException if interrupted while disposing this session
     */
    public synchronized void registerDelayedClose(DelayedClose c) throws ServerException, IOException, InterruptedException {
        if (!started.getAndSet(true)) {
            c.delayedClose();
            empty.set(true);
            return;
        }
        close.set(c);
    }

    /**
     * Wait until the release of the server resource corresponding to the response
     * closed without getting is completed.
     * NOTE: This method must be called with the guarantee that no subsequent add() will be called.
     */
    public synchronized void waitForEmpty() {
        if (started.get()) {
            while (!empty.get()) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    continue;
                }
            }
        }
    }

    private synchronized void notifyEmpty() {
        empty.set(true);
        notify();
    }
}
