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

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.client.SessionAlreadyClosedException;
// import com.tsurugidb.tsubakuro.client.SessionAlreadyClosedException;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * The disposer that disposes server resources corresponding to ForegroundFutureResponses that are closed without being gotten.
 */
public class Disposer extends Thread {
    static final Logger LOG = LoggerFactory.getLogger(Disposer.class);

    private AtomicBoolean started = new AtomicBoolean();

    private AtomicBoolean sessionClosed = new AtomicBoolean();

    private AtomicBoolean queueIsEmpty = new AtomicBoolean(); // FIXME remove this line. Placed to avoid spot bugs wrongly pointed out.

    private Queue<ForegroundFutureResponse<?>> queue = new ArrayDeque<>();

    private ServerResource session;

    /**
     * Creates a new instance.
     * @param session the current session which this blongs to
     */
    public Disposer(@Nonnull ServerResource session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    @Override
    public void run() {
        while (true) {
            ForegroundFutureResponse<?> futureResponse;
            synchronized (queue) {
                futureResponse = queue.poll();
            }
            if (sessionClosed.get() && futureResponse == null) {
                break;
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
                    // Server resource has been disposed by the session close
                    throw new AssertionError("SessionAlreadyClosedException should not occur in the current server implementation");  // FIXME remove this line
                    // continue;
                } catch (TimeoutException e) {
                    // Let's try again
                    queue.add(futureResponse);
                    continue;
                } catch (Exception e) {
                    // should not occur
                    LOG.info(e.getMessage());
                    continue;
                }
            } else {                             // FIXME remove these lines
                notifyQueueIsEmpty();            // 
            }
            if (!sessionClosed.get()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // No problem, it's OK
                }
            }
        }
        try {
            session.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Receive notification that the session is to be closed soon and 
     * let the caller know if the session can be closed immediately.
     * Once this method is called, add() must not be called thereafter.
     * @return true if the session can be closed immediately
     * as the queue that stores unhandled ForegroundFutureResponse is empty
     */
    public boolean prepareCloseAndIsEmpty() {
        synchronized (queue) {
            sessionClosed.set(true);
            return queue.isEmpty();
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
     * Wait until the release of the server resource corresponding to the response 
     * closed without getting is completed.
     * NOTE: This method is for testing only and is temporary
     */
    public synchronized void waitFinishDisposal() {
        while (!queueIsEmpty.get()) {
            try {
                wait();
            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    private synchronized void notifyQueueIsEmpty() {
        queueIsEmpty.set(true);
        notify();
    }
}
