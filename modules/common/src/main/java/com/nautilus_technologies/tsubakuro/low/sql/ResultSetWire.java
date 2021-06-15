package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * ResultSetWire type.
 */
public interface ResultSetWire extends Closeable {
    SchemaProtos.RecordMeta receiveSchemaMetaData() throws IOException;

    abstract class MessagePackInputStream extends InputStream {
	public abstract void disposeUsedData(long length);
    }

    MessagePackInputStream getMessagePackInputStream();
}
