package com.tsurugidb.tsubakuro.channel.ipc.sql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private static native long createNative(long sessionWireHandle, String name) throws IOException;
    private static native ByteBuffer getChunkNative(long handle);
    private static native void disposeUsedDataNative(long handle, long length);
    private static native boolean isEndOfRecordNative(long handle);
    private static native void closeNative(long handle);

    private AtomicLong wireHandle = new AtomicLong(0);  // for c++
    private long sessionWireHandle;
    private ByteBufferBackedInput byteBufferBackedInput;

    class ByteBufferBackedInputForIpc extends ByteBufferBackedInput {
        private final ResultSetWireImpl resultSetWireImpl;

        ByteBufferBackedInputForIpc(ByteBuffer source, ResultSetWireImpl resultSetWireImpl) {
            super(source);
            this.resultSetWireImpl = resultSetWireImpl;
        }

        protected boolean next() {
            synchronized (this) {
                var wh = wireHandle.get();
                if (wh != 0) {
                    if (source.capacity() > 0) {
                        disposeUsedDataNative(wh, source.capacity());
                    }
                    source = getChunkNative(wh);
                    return Objects.nonNull(source);
                }
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            synchronized (this) {
                discardRemainingResultSet();
                super.close();
                resultSetWireImpl.close();
            }
        }

        private void discardRemainingResultSet() {
            var wh = wireHandle.get();
            if (wh == 0) {
                return;
            }
            while (Objects.nonNull(source)) {
                if (source.capacity() > 0) {
                    disposeUsedDataNative(wh, source.capacity());
                }
                source = getChunkNative(wh);
            }
        }
    }

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param sessionWireHandle the handle of the Wire to which the transaction that created this object belongs
     */
    public ResultSetWireImpl(long sessionWireHandle) {
        this.sessionWireHandle = sessionWireHandle;
        this.byteBufferBackedInput = null;
    }

    /**
     * Connect this to the wire specifiec by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     */
    public ResultSetWire connect(String name) throws IOException {
        if (name.length() == 0) {
            throw new IOException("ResultSet wire name is empty");
        }
        synchronized (this) {
            wireHandle.set(createNative(sessionWireHandle, name));
            return this;
        }
    }

    /**
     * Provides the Input to retrieve the received data.
     * @return ByteBufferInput contains the record data from the SQL server.
     */
    public InputStream getByteBufferBackedInput() {
        synchronized (this) {
            if (Objects.isNull(byteBufferBackedInput)) {
                var buffer = getChunkNative(wireHandle.get());
                if (Objects.nonNull(buffer)) {
                    byteBufferBackedInput = new ByteBufferBackedInputForIpc(buffer, this);
                } else {
                    byteBufferBackedInput = new ByteBufferBackedInputForIpc(ByteBuffer.allocate(0), this);
                }
            }
            return byteBufferBackedInput;
        }
    }

    /**
     * Close the wire
     */
    public void close() {
        if (Objects.nonNull(byteBufferBackedInput)) {
            synchronized (byteBufferBackedInput) {
                var wh = wireHandle.get();
                if (wh != 0) {
                    ((ByteBufferBackedInputForIpc) byteBufferBackedInput).discardRemainingResultSet();
                    closeNative(wh);
                }
                wireHandle.set(0);
            }
        } else {
            var wh = wireHandle.getAndSet(0);
            if (wh != 0) {
                closeNative(wh);
            }
        }
    }
}
