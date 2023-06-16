package com.tsurugidb.tsubakuro.channel.ipc.sql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

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


    private long sessionWireHandle;
    private ByteBufferBackedInput byteBufferBackedInput;

    class ByteBufferBackedInputForIpc extends ByteBufferBackedInput {
        private long wireHandle;  // for c++

        ByteBufferBackedInputForIpc(long sessionWireHandle, String name) throws IOException {
            super(ByteBuffer.allocate(0));
            this.wireHandle = createNative(sessionWireHandle, name);
        }

        protected boolean next() {
            synchronized (this) {
                if (wireHandle != 0) {
                    if (source.capacity() > 0) {
                        disposeUsedDataNative(wireHandle, source.capacity());
                    }
                    source = getChunkNative(wireHandle);
                    return Objects.nonNull(source);
                }
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            synchronized (this) {
                if (wireHandle != 0) {
                    discardRemainingResultSet();
                    super.close();
                    closeNative(wireHandle);
                    wireHandle = 0;
                }
            }
        }

        private void discardRemainingResultSet() {
            while (Objects.nonNull(source)) {
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
        byteBufferBackedInput = new ByteBufferBackedInputForIpc(sessionWireHandle, name);
        return this;
    }

    /**
     * Provides the Input to retrieve the received data.
     * @return ByteBufferInput contains the record data from the SQL server.
     */
    public InputStream getByteBufferBackedInput() {
        return byteBufferBackedInput;
    }

    /**
     * Close do nothing
     */
    public void close() {
    }
}
