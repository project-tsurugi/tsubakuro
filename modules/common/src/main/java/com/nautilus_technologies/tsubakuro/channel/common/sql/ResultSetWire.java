package com.nautilus_technologies.tsubakuro.channel.common.sql;

import java.io.Closeable;
import java.io.IOException;
import org.msgpack.core.buffer.ByteBufferInput;

/**
 * ResultSetWire type.
 */
public interface ResultSetWire extends Closeable {
    /**
     * Connect this to the wire specifiec by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     */
    void connect(String name) throws IOException;

    /**
     * Provides an InputStream to retrieve the received data.
     * @return ByteBufferInput contains the record data from the SQL server.
     */
    ByteBufferInput getByteBufferBackedInput();

    /**
     * Disposes the received data in this ResultSetWire
     * @param length the length of the data to be disposed
     * @return true if next recored is expected to receive
     * @throws IOException error occurred in record transfer
     */
    boolean disposeUsedData(long length) throws IOException;
}
