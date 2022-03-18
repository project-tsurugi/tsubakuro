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
     */
    void connect(String name) throws IOException;

    /**
     * Provides an InputStream to retrieve the received data.
     */
    ByteBufferInput getByteBufferBackedInput();

    boolean disposeUsedData(long length) throws IOException;
}
