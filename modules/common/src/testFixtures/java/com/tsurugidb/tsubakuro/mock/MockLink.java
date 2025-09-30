/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ChannelResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;

/**
 * MockLink type.
 */
public final class MockLink extends Link {
    static final Logger LOG = LoggerFactory.getLogger(MockLink.class);

    private final ConcurrentLinkedQueue<ResponseMessage> registerdMessages = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ResponseMessage> readyMessages = new ConcurrentLinkedQueue<>();

    public static final byte RESPONSE_NULL = 0;
    public static final byte RESPONSE_PAYLOAD = 1;
    public static final byte RESPONSE_BODYHEAD = 2;

    private ResponseMessage currentMessage;
    private byte[] justBeforeHeader;
    private byte[] justBeforePayload;
    private boolean alive;
    private MockResultSetWire resultSetWire;
    private boolean timeoutOnEmpty;

    public MockLink() {
        this.alive = true;
        this.timeoutOnEmpty = false;
    }

    @Override
    protected void doSend(int s, @Nonnull byte[] frameHeader, @Nonnull byte[] payload, @Nonnull ChannelResponse channelResponse) {
        justBeforeHeader = frameHeader;
        justBeforePayload = payload;
        if (!alive) {
            channelResponse.setMainResponse(new IOException("MockLink already closed"));
            return;
        }
        if (registerdMessages.isEmpty()) {
            if (timeoutOnEmpty) {
                return;
            }
            throw new AssertionError("no more response message registered");
        }
        while (true) {
            var message = registerdMessages.poll();
            message.assignSlot(s);
            readyMessages.offer(message);
            if (message.getIOException() != null) {
                continue;
            }
            LOG.trace("send {}", payload);
            break;
        }
    }

    @Override
    public boolean doPull(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        currentMessage = readyMessages.peek();
        if (currentMessage != null) {
            if (currentMessage.getIOException() != null) {
                readyMessages.poll();
                throw currentMessage.getIOException();
            }
            if (currentMessage.hasBodyHead()) {
                pushHead(currentMessage.getSlot(), currentMessage.getBodyHead(), createResultSetWire());
                return true;
            }
            if (currentMessage.getInfo() == RESPONSE_PAYLOAD) {
                push(currentMessage.getSlot(), currentMessage.getBytes());
                readyMessages.poll();
                return true;
            }
        }
        if (timeoutOnEmpty) {
            if (timeout > 0) {
                try {
                    Thread.sleep(unit.toMillis(timeout));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while waiting for response", e);
                }
                throw new TimeoutException("MockLink timeout on empty");
            } else {
                while (true) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted while waiting for response", e);
                    }
                }
                // never reach
            }
        }
        return false;
    }

    @Override
    public ResultSetWire createResultSetWire() {
        this.resultSetWire = new MockResultSetWire();
        return resultSetWire;
    }

    @Override
    public boolean isAlive() {
        return alive;
    }

    @Override
    public String linkLostMessage() {
        return "";
    }

    @Override
    public void close() {
        alive = false;
    }

    public MockResultSetWire getResultSetWire() {
        return resultSetWire;
    }

    public boolean next(@Nonnull Message payload) throws IOException {
        return next(toByteArray(payload));
    }

    public boolean next(@Nonnull Message payload, @Nonnull String name, @Nonnull SqlResponse.ResultSetMetadata metadata) throws IOException {
        return registerdMessages.offer(new ResponseMessage(toByteArray(payload),
                                                            toByteArray(SqlResponse.Response.newBuilder()
                                                                            .setExecuteQuery(SqlResponse.ExecuteQuery.newBuilder()
                                                                                                .setName(name)
                                                                                                .setRecordMeta(metadata))
                                                                            .build())));
    }

    private byte[] toByteArray(@Nonnull Message payload) {
        var header = FrameworkResponse.Header.newBuilder().setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT).build();
        try (var buffer = new ByteArrayOutputStream()) {
            header.writeDelimitedTo(buffer);
            payload.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    public boolean next(@Nonnull Message header, @Nonnull Message payload) {
        return next(toByteArray(header, payload));
    }

    public boolean next(@Nonnull Message header, @Nonnull Message payload, @Nonnull String name, @Nonnull SqlResponse.ResultSetMetadata metadata) throws IOException {
        return registerdMessages.offer(new ResponseMessage(toByteArray(header, payload),
                                                    toByteArray(SqlResponse.Response.newBuilder()
                                                                    .setExecuteQuery(SqlResponse.ExecuteQuery.newBuilder()
                                                                                        .setName(name)
                                                                                        .setRecordMeta(metadata))
                                                                    .build())));
    }

    private byte[] toByteArray(@Nonnull Message header, @Nonnull Message payload) {
        try (var buffer = new ByteArrayOutputStream()) {
            header.writeDelimitedTo(buffer);
            payload.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    private boolean next(byte [] responseMessage) {
        return registerdMessages.offer(new ResponseMessage(responseMessage));
    }

    public boolean next(IOException e) {
        return registerdMessages.offer(new ResponseMessage(e));
    }

    public byte[] getJustBeforeHeader() {
        return justBeforeHeader;
    }

    public byte[] getJustBeforePayload() {
        return justBeforePayload;
    }

    public boolean hasRemaining() {
        return !(registerdMessages.isEmpty() && readyMessages.isEmpty());
    }

    public void setTimeoutOnEmpty(boolean to) {
        timeoutOnEmpty = to;
    }
}
