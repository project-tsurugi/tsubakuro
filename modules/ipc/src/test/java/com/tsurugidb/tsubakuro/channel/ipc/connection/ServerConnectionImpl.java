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

import java.io.Closeable;
import java.io.IOException;

import com.tsurugidb.tsubakuro.channel.ipc.sql.ServerWireImpl;

/**
 * ServerConnectionImpl type.
 */
public class ServerConnectionImpl implements Closeable {
    private static native long createNative(String name);
    private static native long listenNative(long handle);
    private static native void acceptNative(long handle, long id);
    private static native void rejectNative(long handle);
    private static native void closeNative(long handle);

    static {
        System.loadLibrary("wire-test");
    }

    private long handle;
    private String name;

    ServerConnectionImpl(String name) throws IOException {
        this.handle = createNative(name);
        this.name = name;
    }

    public long listen() {
        return listenNative(handle);
    }

    public ServerWireImpl accept(long id) throws IOException {
        var rv = new ServerWireImpl(name, id);
        acceptNative(handle, id);
        return rv;
    }

    public void reject() throws IOException {
        rejectNative(handle);
    }

    public void close() throws IOException {
        closeNative(handle);
    }
}
