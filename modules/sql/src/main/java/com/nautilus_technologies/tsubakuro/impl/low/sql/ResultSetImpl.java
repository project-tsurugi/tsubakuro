package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.value.ValueType;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * ResultSetImpl type.
 */
public class ResultSetImpl implements ResultSet {
    private class RecordMetaImpl implements RecordMeta {
	private SchemaProtos.RecordMeta recordMeta;

	RecordMetaImpl(SchemaProtos.RecordMeta recordMeta) {
	    this.recordMeta = recordMeta;
	}
        public CommonProtos.DataType at(int index) throws IOException {
	    if (index < 0 || fieldCount() <= index) {
		throw new IOException("index is out of the range");
	    }
	    return recordMeta.getColumnsList().get(index).getType();
	}
        public CommonProtos.DataType at() throws IOException {
	    if (!columnReady) {
		throw new IOException("the column is not ready to be read");
	    }
	    return recordMeta.getColumnsList().get(columnIndex).getType();
	}
        public boolean nullable(int index) throws IOException {
	    if (index < 0 || fieldCount() <= index) {
		throw new IOException("index is out of the range");
	    }
	    return recordMeta.getColumnsList().get(index).getNullable();
	}
        public boolean nullable() throws IOException {
	    if (!columnReady) {
		throw new IOException("the column is not ready to be read");
	    }
	    return recordMeta.getColumnsList().get(columnIndex).getNullable();
	}
        public long fieldCount() {
	    return recordMeta.getColumnsList().size();
	}
    }

    private ResultSetWire resultSetWire;
    private RecordMetaImpl recordMeta;
    private ResultSetWire.MessagePackInputStream inputStream;
    private MessageUnpacker unpacker;
    private int columnIndex;
    private boolean detectNull;
    private boolean columnReady;
    
    public ResultSetImpl(ResultSetWire resultSetWire) {
	this.resultSetWire = resultSetWire;
    }
	
    public RecordMeta getRecordMeta() throws IOException {
	recordMeta = new RecordMetaImpl(resultSetWire.receiveSchemaMetaData());
	return recordMeta;
    }

    void skipRestOfColumns() throws IOException {
	if (!columnReady) {
	    nextColumn();
	}
	do {
	    if (!isNull()) {
		switch (recordMeta.at(columnIndex)) {
		case INT4:
		    getInt4();
		    break;
		case INT8:
		    getInt8();
		    break;
		case FLOAT4:
		    getFloat4();
		    break;
		case FLOAT8:
		    getFloat8();
		    break;
		case STRING:
		    getCharacter();
		    break;
		default:
		    throw new IOException("the column type is invalid");
		}
	    }
	} while (nextColumn());
    }
    public boolean nextRecord() throws IOException {
	if (unpacker != null) {
	    if (columnIndex != recordMeta.fieldCount()) {
		skipRestOfColumns();
	    }
	    inputStream.disposeUsedData(unpacker.getTotalReadBytes());
	}
	inputStream = resultSetWire.getMessagePackInputStream();
	unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(inputStream);
	columnIndex = -1;
	columnReady = false;
	return unpacker.hasNext();
    }
    public boolean isNull() throws IOException {
	if (recordMeta.nullable(columnIndex)) {
	    MessageFormat format = unpacker.getNextFormat();
	    ValueType type = format.getValueType();
	    if (type == ValueType.NIL) {
		detectNull = true;
		columnReady = false;
		unpacker.unpackNil();
		return true;
	    }
	}
	return false; 
   }
    public int getInt4() throws IOException {
	if (detectNull) {
	    throw new IOException("the column is Null");
	}
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	MessageFormat format = unpacker.getNextFormat();
	ValueType type = format.getValueType();
	if (type != ValueType.INTEGER) {
	    if (type == ValueType.NIL) {
		throw new IOException("the column is Null");
	    }
	    throw new IOException("the column type is not what is expected");
	}
	columnReady = false;
	return unpacker.unpackInt();
    }
    public long getInt8() throws IOException {
	if (detectNull) {
	    throw new IOException("the column is Null");
	}
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	MessageFormat format = unpacker.getNextFormat();
	ValueType type = format.getValueType();
	if (type != ValueType.INTEGER) {
	    if (type == ValueType.NIL) {
		throw new IOException("the column is Null");
	    }
	    throw new IOException("the column type is not what is expected");
	}
	columnReady = false;
	return unpacker.unpackLong();
    }
    public float getFloat4() throws IOException {
	if (detectNull) {
	    throw new IOException("the column is Null");
	}
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	MessageFormat format = unpacker.getNextFormat();
	ValueType type = format.getValueType();
	if (type != ValueType.FLOAT && format != MessageFormat.FLOAT32) {
	    if (type == ValueType.NIL) {
		throw new IOException("the column is Null");
	    }
	    throw new IOException("the column type is not what is expected");
	}
	columnReady = false;
	return unpacker.unpackFloat();
    }
    public double getFloat8() throws IOException {
	if (detectNull) {
	    throw new IOException("the column is Null");
	}
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	MessageFormat format = unpacker.getNextFormat();
	ValueType type = format.getValueType();
	if (type != ValueType.FLOAT && format != MessageFormat.FLOAT64) {
	    if (type == ValueType.NIL) {
		throw new IOException("the column is Null");
	    }
	    throw new IOException("the column type is not what is expected");
	}
	columnReady = false;
	return unpacker.unpackDouble();
    }
    public String getCharacter() throws IOException {
	if (detectNull) {
	    throw new IOException("the column is Null");
	}
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	MessageFormat format = unpacker.getNextFormat();
	ValueType type = format.getValueType();
	if (type != ValueType.STRING) {
	    if (type == ValueType.NIL) {
		throw new IOException("the column is Null");
	    }
	    throw new IOException("the column type is not what is expected");
	}
	columnReady = false;
	return unpacker.unpackString();
    }
    public boolean nextColumn() {
	detectNull = false;
	columnIndex++;
	if (columnIndex < recordMeta.fieldCount()) {
	    columnReady = true;
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
