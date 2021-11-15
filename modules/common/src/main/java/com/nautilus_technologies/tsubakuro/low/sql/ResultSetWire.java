package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import com.nautilus_technologies.tsubakuro.protos.SchemaProtos;

/**
 * ResultSetWire type.
 */
public interface ResultSetWire extends Closeable {
    /**
     * Receiving data sent by MessagePack is supposed to be retrieved via InputStream.
     */
    abstract class MessagePackInputStream extends InputStream {
	public abstract void disposeUsedData(long length) throws IOException;
    }

    /**
     * Connect this to the wire specifiec by the name.
     */
    void connect(String name) throws IOException;

    /**
     * Provides an InputStream to retrieve the received data.
     */
    MessagePackInputStream getMessagePackInputStream();
}
