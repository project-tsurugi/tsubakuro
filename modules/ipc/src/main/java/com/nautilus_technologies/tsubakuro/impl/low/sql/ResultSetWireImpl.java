package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private static native long createNative(long sessionWireHandle, String name);
    private static native byte[] recvMetaNative(long handle);
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
	    byte[] buf = recvMetaNative(wireHandle);
	    return SchemaProtos.RecordMeta.parseFrom(buf);
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    IOException newEx = new IOException("error: ResultSetWireImpl.recvMeta()");
	    newEx.initCause(e);
	    throw newEx;
	}
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
