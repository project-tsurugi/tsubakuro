package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.ByteBufferInput;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;

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

    class ByteBufferBackedInput extends ByteBufferInput {
	ByteBufferBackedInput(ByteBuffer byteBuffer) {
	    super(byteBuffer);
	}

	public MessageBuffer next() {
	    var rv = super.next();
	    if (!Objects.isNull(rv)) {
		return rv;
	    }
	    if (!eor) {
		var buffer = getChunkNative(wireHandle);
		if (Objects.isNull(buffer)) {
		    return null;
		}
		super.reset(buffer);
		return super.next();
	    }
	    return null;
	}
    }

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param sessionWireHandle the handle of the sessionWire to which the transaction that created this object belongs
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
    public ByteBufferInput getByteBufferBackedInput() {
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
	byteBufferBackedInput.reset(buffer);
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
