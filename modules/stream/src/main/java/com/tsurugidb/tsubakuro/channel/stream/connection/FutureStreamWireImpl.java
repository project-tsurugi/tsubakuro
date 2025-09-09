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
package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureStreamWireImpl type.
 */
public class FutureStreamWireImpl implements FutureResponse<Wire> {

    private final StreamLink streamLink;
    private final WireImpl wireImpl;
    private final ClientInformation clientInformation;
    private final AtomicBoolean gotton = new AtomicBoolean();
    private final AtomicReference<Wire> result = new AtomicReference<>();
    private final Lock lock = new ReentrantLock();
    private FutureResponse<Long> futureSessionId = null;
    private boolean closed = false;

    FutureStreamWireImpl(StreamLink streamLink, WireImpl wireImpl, ClientInformation clientInformation) {
        this.streamLink = streamLink;
        this.wireImpl = wireImpl;
        this.clientInformation = clientInformation;
    }

    @Override
    public Wire get() throws IOException, ServerException, InterruptedException {
        try {
            return get(0, null);
        } catch (TimeoutException e) {
            throw new AssertionError("TimeoutException should not have arisen, but it did.");
        }
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws TimeoutException, IOException, ServerException, InterruptedException {
        futureSessionId = wireImpl.handshake(clientInformation, wireInformation(), timeout, unit);
        while (true) {
            var wire = result.get();
            if (wire != null) {
                return wire;
            }
            if (lock.tryLock(timeout, unit)) {
                try {
                    wire = result.get();
                    if (wire != null) {
                        return wire;
                    }
                    if (!gotton.getAndSet(true)) {
                        try {
                            streamLink.setSessionId(futureSessionId.get(timeout, unit));
                            result.set(wireImpl);
                            return wireImpl;
                        } catch (TimeoutException | IOException | ServerException | InterruptedException e) {
                            try {
                                closeInternal();
                            } catch (Exception suppress) {
                                // the exception in closeInternal should be suppressed
                                e.addSuppressed(suppress);
                            }
                            throw e;
                        }
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                throw new TimeoutException("get() by another thread has not returned within the specifined time (" + timeout + " " + unit + ")");
            }
            if (closed) {
                throw new IOException("FutureStreamWireImpl is already closed");
            }
        }
    }

    private EndpointRequest.WireInformation wireInformation() {
        return EndpointRequest.WireInformation.newBuilder().setStreamInformation(
            EndpointRequest.WireInformation.StreamInformation.newBuilder().setMaximumConcurrentResultSets(Link.responseBoxSize())
        ).build();
    }

    @Override
    public boolean isDone() {
        return result.get() != null;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            closeInternal();
        }
    }

    private void closeInternal() throws IOException, InterruptedException, ServerException {
        Exception top = null;
        if (!closed) {
            closed = true;
            if (result.get() == null) {
                try {
                    if (futureSessionId != null) {
                        futureSessionId.close();
                    }
                } catch (Exception e) {
                    top = e;
                } finally {
                    try {
                        streamLink.closeWithoutGet();
                    } catch (Exception e) {
                        if (top == null) {
                            top = e;
                        } else {
                            top.addSuppressed(e);
                        }
                    } finally {
                        try {
                            wireImpl.closeWithoutGet();
                        } catch (Exception e) {
                            if (top == null) {
                                top = e;
                            } else {
                                top.addSuppressed(e);
                            }
                        } finally {
                            if (top != null) {
                                if (top instanceof IOException) {
                                    throw (IOException) top;
                                }
                                if (top instanceof InterruptedException) {
                                    throw (InterruptedException) top;
                                }
                                if (top instanceof ServerException) {
                                    throw (ServerException) top;
                                }
                                if (top instanceof RuntimeException) {
                                    throw (RuntimeException) top;
                                }
                                throw new AssertionError(top);
                            }
                        }
                    }
                }
            }
        }
    }
}
