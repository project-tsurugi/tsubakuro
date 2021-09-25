package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.Objects;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CodingErrorAction;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePack.UnpackerConfig;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.value.ValueType;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.SchemaProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * ResultSetImpl type.
 */
public class ResultSetImpl implements ResultSet {
    /**
     * Store the schema of the result set.
     */
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
    
    /**
     * Class constructor, called from FutureResultSetImpl.
     * @param resultSetWire the wire to transfer schema meta data and contents for this result set.
     */
    public ResultSetImpl(ResultSetWire resultSetWire, SchemaProtos.RecordMeta recordMeta) throws IOException {
	this.recordMeta = new RecordMetaImpl(recordMeta);
	this.resultSetWire = resultSetWire;
    }
	
    /**
     * Provide the metadata object.
     * @returns recordMeta the metadata object
     */
    public RecordMeta getRecordMeta() {
	return recordMeta;
    }

    /**
     * Skips column data in the middle of a record.
     */
    void skipRestOfColumns() throws IOException {
	if (!columnReady) {
	    if (!nextColumn()) {
		return;
	    }
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
		case CHARACTER:
		    getCharacter();
		    break;
		default:
		    throw new IOException("the column type is invalid");
		}
	    }
	} while (nextColumn());
    }

    /**
     * Move the read target to the next record.
     */
    public boolean nextRecord() throws IOException {
	if (Objects.isNull(resultSetWire)) {
            throw new IOException("already closed");
	}
        if (Objects.isNull(unpacker)) {
	    inputStream = resultSetWire.getMessagePackInputStream();
	} else {
	    if (columnIndex != recordMeta.fieldCount()) {
		skipRestOfColumns();
	    }
	    inputStream.disposeUsedData(unpacker.getTotalReadBytes());
	}
	unpacker = new UnpackerConfig()
	    .withActionOnMalformedString(CodingErrorAction.IGNORE)
	    .withActionOnUnmappableString(CodingErrorAction.IGNORE)
	    .newUnpacker(inputStream);
	columnIndex = -1;
	columnReady = false;
	return unpacker.hasNext();
    }

    /**
     * Move the read target to the next column of current record.
     *
     * Reading all columns of all records that a recordSet has is done in the following way.
     *
     *   while (resultSet.nextRecord()) {
     *     while (resultSet.nextColumn()) {
     *       columnData = resultSet.getXXX();
     *       do some processing using columnData;
     *     }
     *   }
     */
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
     * Check if the current column is null.
     * @returns true if the current column is null.
     */
    public boolean isNull() throws IOException {
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
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
    /**
     * Get the value of the current column as an Int4.
     * @returns the value of current column in Int4
     */
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
    /**
     * Get the value of the current column as an Int8.
     * @returns the value of current column in Int8
     */
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
    /**
     * Get the value of the current column as an Float4.
     * @returns the value of current column in Float4
     */
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
    /**
     * Get the value of the current column as an Float8.
     * @returns the value of current column in Float8
     */
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
    /**
     * Get the value of the current column as an Character string.
     * @returns the value of current column in Character string.
     */
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

    /**
     * Close the ResultSetImpl
     */
    public void close() throws IOException {
	resultSetWire.close();
	resultSetWire = null;
    }
}
