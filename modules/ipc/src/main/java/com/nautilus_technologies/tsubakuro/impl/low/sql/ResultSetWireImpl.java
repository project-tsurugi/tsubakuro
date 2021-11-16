package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.ByteBufferInput;
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
    private ByteBufferBackedInput byteBufferBackedInput;

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
	byteBufferBackedInput = null;
    }

    class ByteBufferBackedInput extends ByteBufferInput {
	ByteBufferBackedInput(ByteBuffer byteBuffer) {
	    super(byteBuffer);
	}

	public MessageBuffer next() {
	    var rv = super.next();
	    if (!Objects.isNull(rv)) {
		return rv;
	    }
	    var buffer = getChunkNative(wireHandle);
	    if (Objects.isNull(buffer)) {
		return null;
	    }
	    super.reset(buffer);
	    return super.next();
	}
    }

    /**
     * Provides the Input to retrieve the received data.
     */
    public ByteBufferInput getByteBufferBackedInput() {
	if (Objects.isNull(byteBufferBackedInput)) {
	    var buffer = getChunkNative(wireHandle);
	    if (Objects.isNull(buffer)) {
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
	byteBufferBackedInput.close();
    }
}
