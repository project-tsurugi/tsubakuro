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
package com.tsurugidb.tsubakuro.channel.ipc.connection;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureIpcWireImpl type.
 */
public class FutureIpcWireImpl implements FutureResponse<Wire> {

    private final ClientInformation clientInformation;
    private IpcConnectorImpl connector;
    private long id;
    private final AtomicBoolean gotton = new AtomicBoolean();
    private final AtomicReference<Wire> result = new AtomicReference<>();
    private final Lock lock = new ReentrantLock();
    private final boolean connectException;
    private boolean closed = false;

    FutureIpcWireImpl(IpcConnectorImpl connector, long id, @Nonnull ClientInformation clientInformation) {
        this.connector = connector;
        this.id = id;
        this.clientInformation = clientInformation;
        this.connectException = false;
    }

    FutureIpcWireImpl() {
        this.clientInformation = null;  // do not use when connectException occurs
        this.connectException = true;
    }

    private EndpointRequest.WireInformation wireInformation() {
        return EndpointRequest.WireInformation.newBuilder().setIpcInformation(
            EndpointRequest.WireInformation.IpcInformation.newBuilder().setConnectionInformation(Long.toString(ProcessHandle.current().pid()))
        ).build();
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
        if (connectException) {
            throw new ConnectException("the server has declined the connection request");
        }
        while (true) {
            var wire = result.get();
            if (wire != null) {
                return wire;
            }
            if (unit != null ? lock.tryLock(timeout, unit) : lock.tryLock()) {
                try {
                    wire = result.get();
                    if (wire != null) {
                        return wire;
                    }
                    if (!gotton.getAndSet(true)) {
                        WireImpl wireImpl = null;
                        FutureResponse<Long> futureSessionId = null;
                        try {
                            wireImpl = unit != null ? connector.getSessionWire(id, timeout, unit) : connector.getSessionWire(id);
                            futureSessionId = wireImpl.handshake(clientInformation, wireInformation(), timeout, unit);
                            wireImpl.checkSessionId(unit != null ? futureSessionId.get(timeout, unit) : futureSessionId.get());
                            result.set(wireImpl);
                            return wireImpl;
                        } catch (TimeoutException | IOException | ServerException | InterruptedException e) {
                            closed = true;
                            try {
                                if (futureSessionId != null) {
                                    futureSessionId.close();
                                }
                            } catch (Exception suppress) {
                                // the exception in closing wireImpl should be suppressed
                                e.addSuppressed(suppress);
                            } finally {
                                if (wireImpl != null) {
                                    try {
                                        wireImpl.close();
                                    } catch (Exception suppress) {
                                        // the exception in closing wireImpl should be suppressed
                                        e.addSuppressed(suppress);
                                    }
                                }
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
                throw new IOException("FutureIpcWireImpl is already closed");
            }
        }
    }

    @Override
    public boolean isDone() {
        if (result.get() != null) {
            return true;
        }
        return connector.checkConnection(id);
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            if (!closed) {
                closed = true;
                if (!connectException && result.get() == null) {
                    var wire = connector.getSessionWire(id);
                    wire.close();
                }
            }
        }
    }
}
