package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.tsurugidb.jogasaki.proto.SchemaProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

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
        @Override
        public SqlCommon.AtomType type(int index) throws IOException {
            if (index != 0) {
                throw new IOException("index is out of the range");
            }
            return SqlCommon.AtomType.CHARACTER;
        }
        @Override
        public String name(int index) throws IOException {
            if (index < 0 || fieldCount() <= index) {
                throw new IOException("index is out of the range");
            }
            return "name is not supported";
        }
        @Override
        public boolean nullable(int index) throws IOException {
            if (index != 0) {
                throw new IOException("index is out of the range");
            }
            return false;
        }
        @Override
        public int fieldCount() {
            return 1;
        }
        @Override
        @Deprecated
        public SqlCommon.AtomType at(int index) throws IOException {
            return type(index);
        }
        @Override
        @Deprecated
        public SqlCommon.AtomType at() throws IOException {
            if (!columnReady) {
                throw new IOException("the column is not ready to be read");
            }
            return type(columnIndex);
        }
        @Override
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
    private final FutureResponse<SqlResponse.ResultOnly> futureResponse;

    /**
     * Class constructor, called from FutureResultSetMock.
     */
    public ResultSetMock(boolean isOk) {
        this.recordMeta = new RecordMetaImpl();
        this.numRecords = 0;
        this.futureResponse = new FutureResponseMock(isOk);
    }

    public void connect(String name, SchemaProtos.RecordMeta meta) throws IOException {
        recordMeta = new RecordMetaImpl();
        columnIndex = recordMeta.fieldCount();
    }

    /**
     * Provide the metadata object.
     * @return recordMeta the metadata object
     */
    @Override
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
    @Override
    public boolean nextRecord() throws IOException {
        columnIndex = -1;
        columnReady = false;
        numRecords++;
        return     numRecords == 1;
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
    @Override
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
    @Override
    public SqlCommon.AtomType type() throws IOException {
        if (!columnReady) {
            throw new IOException("the column is not ready to be read");
        }
        return recordMeta.at(columnIndex);
    }

    /**
     * Get the current field type
     * @return current field type
     */
    @Override
    public String name() throws IOException {
        if (!columnReady) {
            throw new IOException("the column is not ready to be read");
        }
        return recordMeta.name(columnIndex);
    }

    /**
     * Get the nullability for the current field
     * @return true if the current field is nullable
     */
    @Override
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
    @Override
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
    @Override
    public int getInt4() throws IOException {
        throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Int8.
     * @return the value of current column in Int8
     * @throws IOException error occurred in retrieving column data
     */
    @Override
    public long getInt8() throws IOException {
        throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Float4.
     * @return the value of current column in Float4
     * @throws IOException error occurred in retrieving column data
     */
    @Override
    public float getFloat4() throws IOException {
        throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Float8.
     * @return the value of current column in Float8
     * @throws IOException error occurred in retrieving column data
     */
    @Override
    public double getFloat8() throws IOException {
        throw new IOException("the column type is not what is expected");
    }
    /**
     * Get the value of the current column as an Character string.
     * @return the value of current column in Character string.
     * @throws IOException error occurred in retrieving column data
     */
    @Override
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

    @Override
    public FutureResponse<SqlResponse.ResultOnly> getResponse() {
        return futureResponse;
    }

    /**
     * Close the ResultSetMock
     */
    @Override
    public void close() {
    }
}