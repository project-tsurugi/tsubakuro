package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.nautilus_technologies.tsubakuro.channel.common.connection.sql.ResultSetWire;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private static native long createNative(long sessionWireHandle) throws IOException;
    private static native void connectNative(long handle, String name) throws IOException;
    private static native ByteBuffer getChunkNative(long handle);
    private static native void disposeUsedDataNative(long handle, long length) throws IOException;
    private static native boolean isEndOfRecordNative(long handle);
    private static native void closeNative(long handle);

    private long wireHandle = 0;  // for c++
    private long sessionWireHandle;
    private ByteBufferBackedInput byteBufferBackedInput;
    private boolean eor;

    class ByteBufferBackedInput extends InputStream {
        private ByteBuffer source;

        /**
         * Creates a new instance.
         * @param source the source buffer
         */
        ByteBufferBackedInput(ByteBuffer source) {
            Objects.requireNonNull(source);
            this.source = source;
        }
    
        @Override
        public int read() {
            while (true) {
                if (source.hasRemaining()) {
                    return source.get() & 0xff;
                }
                if (!next()) {
                    return -1;
                }
            }
        }
    
        @Override
        public int read(byte[] b, int off, int len) {
            int read = 0;
            while (true) {
                int count = Math.min((len - read), source.remaining());
                if (count > 0) {
                    source.get(b, (off + read), count);
                    read += count;
                    if (read == len) {
                        return read;
                    }
                }
                if (!next()) {
                    return -1;
                }
            }
        }
    
        boolean next() {
            source = getChunkNative(wireHandle);
            if (Objects.isNull(source)) {
                return false;
            }
            return true;
        }
    }

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param sessionWireHandle the handle of the Wire to which the transaction that created this object belongs
     */
    public ResultSetWireImpl(long sessionWireHandle) {
    this.sessionWireHandle = sessionWireHandle;
    this.byteBufferBackedInput = null;
    this.eor = false;
    }

    /**
     * Connect this to the wire specifiec by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     */
    public void connect(String name) throws IOException {
        if (name.length() == 0) {
            throw new IOException("ResultSet wire name is empty");
        }
        wireHandle = createNative(sessionWireHandle);
        connectNative(wireHandle, name);
    }

    /**
     * Provides the Input to retrieve the received data.
     * @return ByteBufferInput contains the record data from the SQL server.
     */
    public InputStream getByteBufferBackedInput() {
        if (Objects.isNull(byteBufferBackedInput)) {
            var buffer = getChunkNative(wireHandle);
            if (Objects.isNull(buffer)) {
                eor = true;
                return null;
                }
            byteBufferBackedInput = new ByteBufferBackedInput(buffer);
        }
        return byteBufferBackedInput;
    }

    public boolean disposeUsedData(long length) throws IOException {
        disposeUsedDataNative(wireHandle, length);
        var buffer = getChunkNative(wireHandle);
        if (Objects.isNull(buffer)) {
            eor = true;
            return false;
        }
//        byteBufferBackedInput.reset(buffer);
        return true;
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
        closeNative(wireHandle);
        wireHandle = 0;
        if (!Objects.isNull(byteBufferBackedInput)) {
            byteBufferBackedInput.close();
        }
    }
}
