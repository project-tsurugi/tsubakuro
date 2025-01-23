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
package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

public abstract class Link implements ServerResource {
    private final AtomicBoolean useLink = new AtomicBoolean();
    private final Lock lock = new ReentrantLock();
    private final Condition response = lock.newCondition();
    private long receivedMessageNumber = 0;

    protected ResponseBox responseBox = new ResponseBox(this);
    protected TimeUnit closeTimeUnit;
    protected long closeTimeout = 0;
    protected long sessionId;

    /**
     * Getter of the receivedMessageNumber.
     * @return receivedMessageNumber
     **/
    public long messageNumber() {
        return receivedMessageNumber;
    }

    /**
     * Pull a response message from this link.
     * @param checkedMessageNumber up until the message on that number, the caller checked
     * @param t the timeout value
     * @param u the timeout unit
     * @throws TimeoutException if Timeout error was occurred while pulling response message,
     *      which won't be occured when t is 0
     * @throws IOException if I/O error was occurred while pulling response message
     */
    public void pullMessage(long checkedMessageNumber, long t, TimeUnit u) throws TimeoutException, IOException {
        while (true) {
            if (!useLink.getAndSet(true)) {
                lock.lock();
                try {
                    if (receivedMessageNumber > checkedMessageNumber) {
                        useLink.set(false);
                        response.signalAll();
                        return;
                    }
                } finally {
                    lock.unlock();
                }
                try {
                    if (!doPull(t, u)) {
                        throw new IOException(linkLostMessage());
                    }
                    receivedMessageNumber++;
                } catch (IOException e) {
                    throw e;
                } finally {
                    useLink.set(false);
                }
                lock.lock();
                try {
                    response.signalAll();
                } finally {
                    lock.unlock();
                }
            } else {
                lock.lock();
                try {
                    if (receivedMessageNumber > checkedMessageNumber) {
                        return;
                    }
                    if (!useLink.get()) {
                        continue;
                    }
                    try {
                        if (t == 0) {
                            response.await();
                        } else {
                            if (!response.await(t, u)) {
                                throw new TimeoutException("any response has not arrived within the specifined time");
                            }
                        }
                    } catch (InterruptedException e) {
                        continue;
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * Send request message via this link to the server.
     * An exception raised here is to be stored in the channelResponse.
     * @param s the slot number for the responseBox
     * @param frameHeader the frameHeader of the request
     * @param payload the payload of the request
     * @param channelResponse the channelResponse that will store a response for the request
     */
    public abstract void send(int s, byte[] frameHeader, byte[] payload, ChannelResponse channelResponse);

    /**
     * Create a ResultSetWire without a name, meaning that this link is not connected
     * @return ResultSetWire
     * @throws IOException if I/O error was occurred while creating a ResultSetWire
     */
    public abstract ResultSetWire createResultSetWire() throws IOException;

    /**
     * Getter of the sessionId.
     * @return sessionId
     **/
    public long sessionId() {
        return sessionId;
    }

    /**
     * Sets close timeout.
     * @param t the timeout
     */
    public void setCloseTimeout(Timeout t) {
        closeTimeout = t.value();
        closeTimeUnit = t.unit();
    }

    /**
     * Pull a response message from this link.
     * @param t the timeout value
     * @param u the timeout unit
     * @return true if the pull is successful, otherwise false
     * @throws TimeoutException if Timeout error was occurred while pulling response message,
     *      which won't be occured when t is 0
     * @throws IOException if I/O error was occurred while pulling response message
     */
    public abstract boolean doPull(long t, TimeUnit u) throws TimeoutException, IOException;

    /**
     * Provide dead/alive information of this link
     * @return true when the link is alive
     */
    public abstract boolean isAlive();

    /**
     * Provide error message for link disconnection.
     * @return a String describing the situation of link disconnection
     */
    public abstract String linkLostMessage();

    ResponseBox getResponseBox() {
        return responseBox;
    }

    // to suppress spotbug error
    long value() {
        return this.closeTimeout;
    }
    TimeUnit unit() {
        return this.closeTimeUnit;
    }
}
