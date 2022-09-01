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
    private static native long createNative(long sessionWireHandle) throws IOException;
    private static native void connectNative(long handle, String name) throws IOException;
    private static native ByteBuffer getChunkNative(long handle);
    private static native void disposeUsedDataNative(long handle, long length);
    private static native boolean isEndOfRecordNative(long handle);
    private static native void closeNative(long handle);

    private long wireHandle = 0;  // for c++
    private long sessionWireHandle;
    private ByteBufferBackedInput byteBufferBackedInput;

    class ByteBufferBackedInputForIpc extends ByteBufferBackedInput {
        private final ResultSetWireImpl resultSetWireImpl;

        ByteBufferBackedInputForIpc(ByteBuffer source, ResultSetWireImpl resultSetWireImpl) {
            super(source);
            this.resultSetWireImpl = resultSetWireImpl;
        }

        protected boolean next() {
            disposeUsedDataNative(wireHandle, source.capacity());
            source = getChunkNative(wireHandle);
            if (Objects.isNull(source)) {
                return false;
            }
            return true;
        }
        
        @Override
        public void close() throws IOException {
            super.close();
            resultSetWireImpl.close();
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
        wireHandle = createNative(sessionWireHandle);
        connectNative(wireHandle, name);
        return this;
    }

    /**
     * Provides the Input to retrieve the received data.
     * @return ByteBufferInput contains the record data from the SQL server.
     */
    public InputStream getByteBufferBackedInput() {
        if (Objects.isNull(byteBufferBackedInput)) {
            var buffer = getChunkNative(wireHandle);
            if (Objects.isNull(buffer)) {
                close();
                return null;
            }
            byteBufferBackedInput = new ByteBufferBackedInputForIpc(buffer, this);
        }
        return byteBufferBackedInput;
    }

    /**
     * Close the wire
     */
    public void close() {
        if (wireHandle != 0) {
            closeNative(wireHandle);
            wireHandle = 0;
        }
    }
}
