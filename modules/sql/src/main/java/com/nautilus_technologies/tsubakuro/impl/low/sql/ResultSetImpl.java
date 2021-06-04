package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * ResultSetImpl type.
 */
public class ResultSetImpl implements ResultSet {
    class RecordMetaImpl implements RecordMeta {
        public CommonProtos.DataType at(int index) {
	    return recordMeta.getColumnsList().get(index).getType();
	}
        public boolean nullable(int index) {
	    return recordMeta.getColumnsList().get(index).getNullable();
	}
        public long fieldCount() {
	    return recordMeta.getColumnsList().size();
	}
    }

    private ResultSetWire resultSetWire;
    private SchemaProtos.RecordMeta recordMeta;

    public ResultSetImpl(ResultSetWire w) {
	resultSetWire = w;
    }
	
    public RecordMeta getRecordMeta() throws IOException {
	recordMeta = resultSetWire.recvMeta();
	return new RecordMetaImpl();
    }
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
