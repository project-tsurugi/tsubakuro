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
    private static native long createNative(long sessionWireHandle, String name);
    private static native ByteBuffer recvMetaNative(long handle);
    private static native ByteBuffer getChunkNative(long handle);
    private static native void disposeNative(long handle, long length);
    private static native boolean isEndOfRecordNative(long handle);
    private static native boolean closeNative(long handle);

    private long wireHandle = 0;  // for c++

    public ResultSetWireImpl(long sessionWireHandle, String name) throws IOException {
	wireHandle = createNative(sessionWireHandle, name);
	if (wireHandle == 0) {
	    throw new IOException("error: ResultSetWireImpl.ResultSetWireImpl()");
	}
    }

    public SchemaProtos.RecordMeta recvMeta() throws IOException {
	try {
	    ByteBuffer buf = recvMetaNative(wireHandle);
	    return SchemaProtos.RecordMeta.parseFrom(buf);
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    throw new IOException("error: ResultSetWireImpl.recvMeta()", e);
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
	public synchronized void dispose(long length) {
	    ResultSetWireImpl.disposeNative(wireHandle, length);	    
	}
    }

    public MessagePackInputStream getMessagePackInputStream() {
	return new ByteBufferBackedInputStream();
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
	if (wireHandle != 0) {
	    if (!closeNative(wireHandle)) {
		throw new IOException("error: ResultSetWireImpl.close()");
	    }
	    wireHandle = 0;
	}
    }
}
