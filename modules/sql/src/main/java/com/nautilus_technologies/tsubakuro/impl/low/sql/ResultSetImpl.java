package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * ResultSetImpl type.
 */
public class ResultSetImpl implements ResultSet {
    class RecordMetaImpl implements RecordMeta {
        public CommonProtos.DataType at(int index) {
	    return CommonProtos.DataType.INT8;
	}
        public boolean nullable(int index) {
	    return true;
	}
        public long fieldCount() {
	    return 3;
	}
    }

    private RecordMetaImpl recordMetaImpl;
    private ResultSetWire resultSetWire;

    public ResultSetImpl(ResultSetWire w) {
	resultSetWire = w;
    }
	
    public RecordMeta getRecordMeta() { return recordMetaImpl; }
    public boolean nextRecord() { return true; }
    public boolean isNull() { return false; }
    public int getInt4() { return 4; }
    public long getInt8() { return 8; }
    public float getFloat4() { return (float) 4.0; }
    public double getFloat8() { return 8.0; }
    public String getCharacter() { return new String("String"); }

    public boolean nextColumn() { return false; }

    /**
     * Close the ResultSetImpl
     */
    public void close() throws IOException {
    }
}
