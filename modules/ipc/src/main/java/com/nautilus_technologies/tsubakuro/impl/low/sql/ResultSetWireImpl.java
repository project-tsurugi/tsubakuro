package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.SchemaProtos;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private static native long createNative(long sessionWireHandle, String name) throws IOException;
    private static native ByteBuffer getChunkNative(long handle);
    private static native void disposeUsedDataNative(long handle, long length) throws IOException;
    private static native boolean isEndOfRecordNative(long handle);
    private static native void closeNative(long handle);

    private long wireHandle = 0;  // for c++

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param sessionWireHandle the handle of the sessionWire to which the transaction that created this object belongs
     * @param name the name of the ResultSetWireImpl to be created
     */
    public ResultSetWireImpl(long sessionWireHandle, String name) throws IOException {
	if (name.length() == 0) {
	    throw new IOException("ResultSet wire name is empty");
	}
	wireHandle = createNative(sessionWireHandle, name);
    }

    /**
     * InputStream class to provide received record data coded by MessagePack.
     */
    class ByteBufferBackedInputStream extends MessagePackInputStream {
	ByteBuffer buf;
	boolean eor;

	ByteBufferBackedInputStream() {
	    buf = ResultSetWireImpl.getChunkNative(wireHandle);
	    eor = (buf == null);
	}
	public synchronized int read() throws IOException {
	    if (eor) {
		return -1;
	    }
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
	    if (eor) {
		return -1;
	    }
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
	public synchronized void disposeUsedData(long length) throws IOException {
	    ResultSetWireImpl.disposeUsedDataNative(wireHandle, length);	    
	}
    }

    /**
     * Provides the InputStream to retrieve the received data.
     */
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
