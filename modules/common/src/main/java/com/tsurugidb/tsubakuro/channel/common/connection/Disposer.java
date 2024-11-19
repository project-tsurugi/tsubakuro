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
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * The disposer that disposes server resources corresponding to ForegroundFutureResponses that are closed without being gotten.
 */
public class Disposer extends Thread {
    static final Logger LOG = LoggerFactory.getLogger(Disposer.class);

    private AtomicBoolean started = new AtomicBoolean();

    private Queue<ForegroundFutureResponse<?>> queue = new ArrayDeque<>();

    private AtomicBoolean finished = new AtomicBoolean();

    private final AtomicReference<CleanUp> cleanUp = new AtomicReference<>();

    /**
     * Enclodure of delayed clean up procedure.
     */
    public interface CleanUp {
        /**
         * clean up procedure.
         * @throws IOException An error was occurred while cleanUP() is executed.
         */
        void cleanUp() throws IOException;
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
            synchronized (queue) {
                futureResponse = queue.poll();
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
                    queue.add(futureResponse);
                    continue;
                } catch (Exception e) {     // should not occur
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                    continue;
                }
            } else {
                if (cleanUp.get() != null) {
                    break;
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // No problem, it's OK
            }
        }

        try {
            cleanUp.get().cleanUp();
        } catch (IOException e) {
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
        }

        if (exception != null) {
            LOG.info(exception.getMessage());
            throw new UncheckedIOException(new IOException(exception));
        }
    }

    void add(ForegroundFutureResponse<?> futureResponse) {
        if (!started.getAndSet(true)) {
            this.start();
        }
        synchronized (queue) {
            queue.add(futureResponse);
        }
    }

    /**
     * Register a clean up procesure.
     * If disposer thread has not started, cleanUp() is executed.
     * NOTE: This method is assumed to be called only in close and/or shutdown of a Session.
     * @param c the clean up procesure to be registered
     * @throws IOException An error was occurred when CleanUP c has been immediately executed.
     */
    public void registerCleanUp(CleanUp c) throws IOException {
        if (!started.getAndSet(true)) {
            c.cleanUp();
            return;
        }
        cleanUp.set(c);
    }

    /**
     * Wait until the release of the server resource corresponding to the response
     * closed without getting is completed.
     * NOTE: This method must be called with the guarantee that no subsequent add() will be called.
     */
    public synchronized void waitForFinish() {
        if (started.get()) {
            while (finished.get()) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    continue;
                }
            }
        }
    }

    private synchronized void notifyFinish() {
        finished.set(false);
        notify();
    }
}
