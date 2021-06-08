package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.value.ValueType;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * ResultSetImpl type.
 */
public class ResultSetImpl implements ResultSet {
    static class RecordMetaImpl implements RecordMeta {
	private SchemaProtos.RecordMeta recordMeta;

	RecordMetaImpl(SchemaProtos.RecordMeta r) {
	    recordMeta = r;
	}
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
    private RecordMetaImpl recordMetaImpl;
    private ResultSetWire.MsgPackInputStream inputStream;
    private MessageUnpacker unpacker;
    private int columnIndex;
    private boolean detectNull;
    
    public ResultSetImpl(ResultSetWire w) {
	resultSetWire = w;
    }
	
    public RecordMeta getRecordMeta() throws IOException {
	recordMetaImpl = new RecordMetaImpl(resultSetWire.recvMeta());
	return recordMetaImpl;
    }

    public boolean nextRecord() throws IOException {
	if (unpacker == null) {
	    inputStream = resultSetWire.getMsgPackInputStream();
	    unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(inputStream);
	} else {
	    inputStream.dispose(unpacker.getTotalReadBytes());
	}
	columnIndex = 0;
	return unpacker.hasNext();
    }
    public boolean isNull() throws IOException {
	if (recordMetaImpl.nullable(columnIndex)) {
	    MessageFormat format = unpacker.getNextFormat();
	    ValueType type = format.getValueType();
	    if (type == ValueType.NIL) {
		unpacker.unpackNil();
		
		return true;
	    }
	}
	return false; 
   }
    public int getInt4() throws IOException {
	if (detectNull) {
	    throw new IOException("column is Null");
	}
	return unpacker.unpackInt();
    }
    public long getInt8() throws IOException {
	if (detectNull) {
	    throw new IOException("column is Null");
	}
	return unpacker.unpackLong();
    }
    public float getFloat4() throws IOException {
	if (detectNull) {
	    throw new IOException("column is Null");
	}
	return unpacker.unpackFloat();
    }
    public double getFloat8() throws IOException {
	if (detectNull) {
	    throw new IOException("column is Null");
	}
	return unpacker.unpackDouble();
    }
    public String getCharacter() throws IOException {
	if (detectNull) {
	    throw new IOException("column is Null");
	}
	return unpacker.unpackString();
    }
    public boolean nextColumn() {
	detectNull = false;
	if (columnIndex < recordMetaImpl.fieldCount()) {
	    return true;
	}
	return false;
    }

    /**
     * Close the ResultSetImpl
     */
    public void close() throws IOException {
    }
}
