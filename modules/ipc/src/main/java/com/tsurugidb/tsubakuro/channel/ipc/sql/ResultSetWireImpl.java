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
package com.tsurugidb.tsubakuro.channel.ipc.sql;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.ipc.IpcLink;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private static native long createNative(long sessionWireHandle, String name) throws IOException;
    private static native ByteBuffer getChunkNative(long handle, long timeoutNs) throws IOException;
    private static native void disposeUsedDataNative(long handle, long length);
    private static native boolean isEndOfRecordNative(long handle);
    private static native void closeNative(long handle);

    private final IpcLink link;
    private long sessionWireHandle;
    private ByteBufferBackedInput byteBufferBackedInput;

    class ByteBufferBackedInputForIpc extends ByteBufferBackedInput {
        private final AtomicBoolean closed = new AtomicBoolean();
        private long wireHandle;  // for c++

        ByteBufferBackedInputForIpc(long sessionWireHandle, String name) throws IOException {
            this.wireHandle = createNative(sessionWireHandle, name);
        }

        @Override
        protected boolean next() throws IOException {
            synchronized (this) {
                if (wireHandle != 0) {
                    if (source.capacity() > 0) {
                        disposeUsedDataNative(wireHandle, source.capacity());
                    }
                    source = getChunkNative(wireHandle, timeoutNanos);
                    return source != null;
                }
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed.getAndSet(true)) {
                synchronized (this) {
                    super.close();
                    closeNative(wireHandle);
                    link.remove(ResultSetWireImpl.this);
                    wireHandle = 0;
                }
            }
        }
    }

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param sessionWireHandle the handle of the Wire to which the transaction that created this object belongs
     * @param link the link through which the result set will be sent
     */
    public ResultSetWireImpl(long sessionWireHandle, IpcLink link) {
        this.sessionWireHandle = sessionWireHandle;
        this.byteBufferBackedInput = null;
        this.link = link;
    }

    /**
     * Connect this to the wire specifiec by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     */
    @Override
    public ResultSetWire connect(String name) throws IOException {
        if (name.length() == 0) {
            throw new IOException("ResultSet wire name is empty");
        }
        byteBufferBackedInput = new ByteBufferBackedInputForIpc(sessionWireHandle, name);
        return this;
    }

    /**
     * Provides the Input to retrieve the received data.
     * @return ByteBufferInput contains the record data from the SQL server.
     */
    @Override
    public InputStream getByteBufferBackedInput() {
        if (byteBufferBackedInput != null) {
            return byteBufferBackedInput;
        }
        return new ByteArrayInputStream(new byte[0]);
    }

    /**
     * Close do nothing
     */
    @Override
    public void close() throws IOException {
        if (byteBufferBackedInput != null) {
            byteBufferBackedInput.close();
            byteBufferBackedInput = null;
        }
    }
}
