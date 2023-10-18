package com.tsurugidb.tsubakuro.channel.ipc.sql;

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
    private static native ByteBuffer getChunkNative(long handle);
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
            super(ByteBuffer.allocate(0));
            this.wireHandle = createNative(sessionWireHandle, name);
        }

        @Override
        protected boolean next() {
            synchronized (this) {
                if (wireHandle != 0) {
                    if (source.capacity() > 0) {
                        disposeUsedDataNative(wireHandle, source.capacity());
                    }
                    source = getChunkNative(wireHandle);
                    return source != null;
                }
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed.getAndSet(true)) {
                synchronized (this) {
                    discardRemainingResultSet();
                    super.close();
                    closeNative(wireHandle);
                    link.remove(ResultSetWireImpl.this);
                    wireHandle = 0;
                }
            }
        }

        private void discardRemainingResultSet() {
            while (source != null) {
                if (source.capacity() > 0) {
                    disposeUsedDataNative(wireHandle, source.capacity());
                }
                source = getChunkNative(wireHandle);
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
        return byteBufferBackedInput;
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
