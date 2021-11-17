package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.msgpack.core.buffer.ByteBufferInput;
import com.nautilus_technologies.tsubakuro.protos.SchemaProtos;

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
