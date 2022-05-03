package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.nio.charset.CodingErrorAction;
import java.util.Objects;

import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack.UnpackerConfig;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.SchemaProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * ResultSetImpl type.
 */
public class ResultSetImpl implements ResultSet {
    /**
     * Store the schema of the result set.
     */
    private class RecordMetaImpl implements RecordMeta {
        private final SchemaProtos.RecordMeta recordMeta;

        RecordMetaImpl(SchemaProtos.RecordMeta recordMeta) {
            this.recordMeta = recordMeta;
        }
        RecordMetaImpl() {
            this.recordMeta = null;
        }
        @Override
        public CommonProtos.DataType type(int index) throws IOException {
            if (index < 0 || fieldCount() <= index) {
                throw new IOException("index is out of the range");
            }
            return recordMeta.getColumnsList().get(index).getType();
        }
        @Override
        public String name(int index) throws IOException {
            if (index < 0 || fieldCount() <= index) {
                throw new IOException("index is out of the range");
            }
            return recordMeta.getColumnsList().get(index).getName();
        }
        @Override
        public boolean nullable(int index) throws IOException {
            if (index < 0 || fieldCount() <= index) {
                throw new IOException("index is out of the range");
            }
            return recordMeta.getColumnsList().get(index).getNullable();
        }
        @Override
        public int fieldCount() {
            if (Objects.isNull(recordMeta)) {
                return 0;
            }
            return recordMeta.getColumnsList().size();
        }
        @Override
        @Deprecated
        public CommonProtos.DataType at(int index) throws IOException {
            if (index < 0 || fieldCount() <= index) {
                throw new IOException("index is out of the range");
            }
            return recordMeta.getColumnsList().get(index).getType();
        }
        @Override
        @Deprecated
        public CommonProtos.DataType at() throws IOException {
            if (!columnReady) {
                throw new IOException("the column is not ready to be read");
            }
            return recordMeta.getColumnsList().get(columnIndex).getType();
        }
        @Override
        @Deprecated
        public boolean nullable() throws IOException {
            if (!columnReady) {
                throw new IOException("the column is not ready to be read");
            }
            return recordMeta.getColumnsList().get(columnIndex).getNullable();
        }
    }

    private ResultSetWire resultSetWire;
    private RecordMetaImpl recordMeta;
    private UnpackerConfig unpackerConfig;
    private MessageUnpacker unpacker;
    private int columnIndex;
    private boolean detectNull;
    private boolean columnReady;
    private boolean recordReady;
    private FutureResponse<ResponseProtos.ResultOnly> futureResponse;

    /**
     * Class constructor, called from FutureResultSetImpl.
     * @param resultSetWire the wire to transfer schema meta data and contents for this result set.
     * @throws IOException error occurred in class constructor
     */
    public ResultSetImpl(ResultSetWire resultSetWire, FutureResponse<ResponseProtos.ResultOnly> futureResponse) throws IOException {
        this.resultSetWire = resultSetWire;
        this.futureResponse = futureResponse;
        unpackerConfig = new UnpackerConfig()
                .withActionOnMalformedString(CodingErrorAction.IGNORE)
                .withActionOnUnmappableString(CodingErrorAction.IGNORE);
    }

    public ResultSetImpl(FutureResponse<ResponseProtos.ResultOnly> futureResponse) throws IOException {
        recordMeta = new RecordMetaImpl();
    }

    public void connect(String name, SchemaProtos.RecordMeta meta) throws IOException {
        recordMeta = new RecordMetaImpl(meta);
        columnIndex = recordMeta.fieldCount();
        resultSetWire.connect(name);
        var byteBufferBackedInput = resultSetWire.getByteBufferBackedInput();
        if (!Objects.isNull(byteBufferBackedInput)) {
            unpacker = unpackerConfig.newUnpacker(byteBufferBackedInput);
        }
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
        if (Objects.isNull(resultSetWire)) {
            throw new IOException("already closed");
        }
        if (Objects.isNull(unpacker)) {  // means the query returns no record
            return false;
        }
        if (columnIndex != recordMeta.fieldCount()) {
            skipRestOfColumns();
        }
        var length = unpacker.getTotalReadBytes();
        if (length > 0) {
            if (!resultSetWire.disposeUsedData(unpacker.getTotalReadBytes())) {
                return false;
            }
            unpacker.reset(resultSetWire.getByteBufferBackedInput());
        }
        columnIndex = -1;
        columnReady = false;
        recordReady = false;
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
    @Override
    public boolean nextColumn() {
        detectNull = false;
        columnIndex++;
        if (columnIndex < recordMeta.fieldCount()) {
            columnReady = true;
            recordReady = true;
            return true;
        }
        columnReady = false;
        recordReady = false;
        return false;
    }

    /**
     * Get the current field type
     * @return current field type
     */
    @Override
    public CommonProtos.DataType type() throws IOException {
        if (!recordReady) {
            throw new IOException("the column is not ready to be read");
        }
        return recordMeta.type(columnIndex);
    }

    /**
     * Get the current field type
     * @return current field type
     */
    @Override
    public String name() throws IOException {
        if (!recordReady) {
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
        if (!recordReady) {
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
     * @return the value of current column in Int4
     * @throws IOException error occurred in retrieving column data
     */
    @Override
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
     * @return the value of current column in Int8
     * @throws IOException error occurred in retrieving column data
     */
    @Override
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
     * @return the value of current column in Float4
     * @throws IOException error occurred in retrieving column data
     */
    @Override
    public float getFloat4() throws IOException {
        if (detectNull) {
            throw new IOException("the column is Null");
        }
        if (!columnReady) {
            throw new IOException("the column is not ready to be read");
        }
        MessageFormat format = unpacker.getNextFormat();
        ValueType type = format.getValueType();
        if (!(type == ValueType.FLOAT && format == MessageFormat.FLOAT32)) {
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
     * @return the value of current column in Float8
     * @throws IOException error occurred in retrieving column data
     */
    @Override
    public double getFloat8() throws IOException {
        if (detectNull) {
            throw new IOException("the column is Null");
        }
        if (!columnReady) {
            throw new IOException("the column is not ready to be read");
        }
        MessageFormat format = unpacker.getNextFormat();
        ValueType type = format.getValueType();
        if (!(type == ValueType.FLOAT && format == MessageFormat.FLOAT64)) {
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
     * @return the value of current column in Character string.
     * @throws IOException error occurred in retrieving column data
     */
    @Override
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
     * Get a FutureResponse of the response returned from the SQL service
     * @return a FutureResponse of ResponseProtos.ResultOnly indicate whether the SQL service has successfully completed processing or not
     */
    @Override
    public FutureResponse<ResponseProtos.ResultOnly> getResponse() {
        return futureResponse;
    }

    /**
     * Close the ResultSetImpl
     * @throws IOException error occurred in close of the resultSet
     */
    @Override
    public void close() throws IOException {
        if (Objects.nonNull(resultSetWire)) {
            resultSetWire.close();
            resultSetWire = null;
        }
    }
}
