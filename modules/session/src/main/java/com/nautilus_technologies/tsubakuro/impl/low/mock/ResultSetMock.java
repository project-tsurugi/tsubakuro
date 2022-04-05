package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.SchemaProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * ResultSetMock type.
 */
public class ResultSetMock implements ResultSet {
    /**
     * Store the schema of the result set.
     */
    private class RecordMetaImpl implements RecordMeta {
	RecordMetaImpl() {
	}
        public CommonProtos.DataType type(int index) throws IOException {
	    if (index != 0) {
		throw new IOException("index is out of the range");
	    }
	    return CommonProtos.DataType.CHARACTER;
	}
        public boolean nullable(int index) throws IOException {
	    if (index != 0) {
		throw new IOException("index is out of the range");
	    }
	    return false;
	}
        public long fieldCount() {
	    return 1;
	}
	@Deprecated
        public CommonProtos.DataType at(int index) throws IOException {
	    return type(index);
	}
	@Deprecated
        public CommonProtos.DataType at() throws IOException {
	    if (!columnReady) {
		throw new IOException("the column is not ready to be read");
	    }
	    return type(columnIndex);
	}
	@Deprecated
        public boolean nullable() throws IOException {
	    if (!columnReady) {
		throw new IOException("the column is not ready to be read");
	    }
	    return nullable(columnIndex);
	}
    }

    public static final String FILE_NAME = "/dump_directory/some_file";
    private RecordMetaImpl recordMeta;
    private int columnIndex;
    private int numRecords;
    private boolean columnReady;
    
    /**
     * Class constructor, called from FutureResultSetMock.
     */
    public ResultSetMock() {
	this.recordMeta = new RecordMetaImpl();
	this.numRecords = 0;
    }

    public void connect(String name, SchemaProtos.RecordMeta meta) throws IOException {
	recordMeta = new RecordMetaImpl();
	columnIndex = (int) recordMeta.fieldCount();
    }

    /**
     * Provide the metadata object.
     * @return recordMeta the metadata object
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
     * @throws IOException error occurred in record receive
     */
    public boolean nextRecord() throws IOException {
	columnIndex = -1;
	columnReady = false;
	numRecords++;
	return 	numRecords == 1;
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
	columnIndex++;
	if (columnIndex < 1) {
	    columnReady = true;
	    return true;
	}
	return false;
    }

    /**
     * Get the current field type
     * @return current field type
     */
    public CommonProtos.DataType type() throws IOException {
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	return recordMeta.at(columnIndex);
    }

    /**
     * Get the nullability for the current field
     * @return true if the current field is nullable
     */
    public boolean nullable() throws IOException {
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	return recordMeta.nullable(columnIndex);
    }

    /**
     * Check if the current column is null.
     * @return true if the current column is null.
     * @throws IOException error occurred in column check
     */
    public boolean isNull() throws IOException {
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	return false; 
    }
    /**
     * Get the value of the current column as an Int4.
     * @return the value of current column in Int4
     * @throws IOException error occurred in retrieving column data
     */
    public int getInt4() throws IOException {
	throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Int8.
     * @return the value of current column in Int8
     * @throws IOException error occurred in retrieving column data
     */
    public long getInt8() throws IOException {
	throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Float4.
     * @return the value of current column in Float4
     * @throws IOException error occurred in retrieving column data
     */
    public float getFloat4() throws IOException {
	throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Float8.
     * @return the value of current column in Float8
     * @throws IOException error occurred in retrieving column data
     */
    public double getFloat8() throws IOException {
	throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Character string.
     * @return the value of current column in Character string.
     * @throws IOException error occurred in retrieving column data
     */
    public String getCharacter() throws IOException {
	if (!columnReady) {
	    throw new IOException("the column is not ready to be read");
	}
	if (columnIndex == 0 && numRecords == 1) {
	    columnReady = false;
	    return FILE_NAME;
	}
	throw new IOException("someshing wrong");
    }

    /**
     * Close the ResultSetMock
     */
    public void close() {
    }
}
