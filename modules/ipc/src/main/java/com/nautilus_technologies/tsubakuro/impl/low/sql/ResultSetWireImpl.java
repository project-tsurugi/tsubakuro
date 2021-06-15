package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private static native long createNative(long sessionWireHandle, String name) throws IOException;
    private static native ByteBuffer receiveSchemaMetaDataNative(long handle);
    private static native ByteBuffer getChunkNative(long handle);
    private static native void disposeUsedDataNative(long handle, long length);
    private static native boolean isEndOfRecordNative(long handle);
    private static native void closeNative(long handle);

    private long wireHandle = 0;  // for c++

    public ResultSetWireImpl(long sessionWireHandle, String name) throws IOException {
	wireHandle = createNative(sessionWireHandle, name);
    }

    public SchemaProtos.RecordMeta receiveSchemaMetaData() throws IOException {
	try {
	    ByteBuffer buf = receiveSchemaMetaDataNative(wireHandle);
	    return SchemaProtos.RecordMeta.parseFrom(buf);
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException("error: ResultSetWireImpl.receiveSchemaMetaData()", e);
	}
    }

    class ByteBufferBackedInputStream extends MessagePackInputStream {
	ByteBuffer buf;

	ByteBufferBackedInputStream() {
	    buf = ResultSetWireImpl.getChunkNative(wireHandle);
	}
	public synchronized int read() throws IOException {
	    if (!buf.hasRemaining()) {
		buf = ResultSetWireImpl.getChunkNative(wireHandle);
		if (buf == null) {
		    if (ResultSetWireImpl.isEndOfRecordNative(wireHandle)) {
			return -1;
		    }
		    throw new IOException("info: Record has not been arrived at ResultSetWireImpl");  //  FIXME Record has not been arrived
		}
	    }
	    return buf.get();
	}
	public synchronized int read(byte[] bytes, int off, int len) throws IOException {
	    if (!buf.hasRemaining()) {
		buf = ResultSetWireImpl.getChunkNative(wireHandle);
		if (buf == null) {
		    if (ResultSetWireImpl.isEndOfRecordNative(wireHandle)) {
			return -1;
		    }
		    throw new IOException("info: Record has not been arrived at ResultSetWireImpl");  //  FIXME Record has not been arrived
		}
	    }
	    len = Math.min(len, buf.remaining());
	    buf.get(bytes, off, len);
	    return len;
	}
	public synchronized void disposeUsedData(long length) {
	    ResultSetWireImpl.disposeUsedDataNative(wireHandle, length);	    
	}
    }

    public MessagePackInputStream getMessagePackInputStream() {
	return new ByteBufferBackedInputStream();
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
	closeNative(wireHandle);
	wireHandle = 0;
    }
}
