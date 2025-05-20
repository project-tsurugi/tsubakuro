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

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.ipc.NativeLibrary;
import com.tsurugidb.tsubakuro.channel.ipc.IpcLink;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * IpcConnectorImpl type.
 */
public final class IpcConnectorImpl implements Connector {
    private static final long MAX_TIMEOUT_YEAR = 10;

    private static final Logger LOG = LoggerFactory.getLogger(IpcConnectorImpl.class);

    private static native long getConnectorNative(String name) throws IOException;
    private static native long requestNative(long handle) throws IOException;
    private static native long waitNative(long handle, long id) throws ConnectException;
    private static native long waitNative(long handle, long id, long timeout) throws ConnectException, TimeoutException;
    private static native boolean checkNative(long handle, long id);
    private static native void closeConnectorNative(long handle);

    private final String name;
    private long handle;
    private int useCount;

    static {
        NativeLibrary.load();
    }

    public IpcConnectorImpl(String name) {
        this.name = name;
    }

    @Override
    public synchronized FutureResponse<Wire> connect(@Nonnull ClientInformation clientInformation) throws IOException {
        LOG.trace("will connect to {}", name); //$NON-NLS-1$
        if (handle == 0) {
            handle = getConnectorNative(name);
        }
        useCount++;
        try {
            long id = requestNative(handle);
            return new FutureIpcWireImpl(this, id, clientInformation);
        } catch (IOException e) {
            return new FutureIpcWireImpl();  // a future that throws ConnectException on get()
        }
    }

    synchronized WireImpl getSessionWire(long id) throws IOException {
        long sessionId = waitNative(handle, id);
        close();
        return new WireImpl(new IpcLink(name, sessionId));
    }

    synchronized WireImpl getSessionWire(long id, long timeout, TimeUnit unit) throws TimeoutException, IOException {
        long timeoutNano = ((MAX_TIMEOUT_YEAR * 365) > TimeUnit.DAYS.convert(timeout, unit)) ? unit.toNanos(timeout) : MAX_TIMEOUT_YEAR * 365 * 24 * 3600_000_000_000L;
        long sessionId = waitNative(handle, id, timeoutNano);
        close();
        return new WireImpl(new IpcLink(name, sessionId));
    }

    synchronized boolean checkConnection(long id) {
        if (handle == 0) {
            return true;
        }
        return checkNative(handle, id);
    }

    private void close() {
        useCount--;
        if (useCount == 0) {
            closeConnectorNative(handle);
            handle = 0;
        }
    }
}