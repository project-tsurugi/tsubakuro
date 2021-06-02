package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private long wireHandle = 0;  // for c++

    public ResultSetWireImpl(String name) throws IOException {
    }

    public SchemaProtos.RecordMeta recvMeta() {
	return SchemaProtos.RecordMeta.newBuilder().build();
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
    }
}
