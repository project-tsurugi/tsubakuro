package com.nautilus_technologies.tsubakuro.impl.low.sql.testing;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

//import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.RelationCursor;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetMetadata;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.nautilus_technologies.tsubakuro.util.Timeout;

/**
 * A basic implementation of {@link ResultSet} which just delegate operations to {@link RelationCursor}.
 */
public class BasicResultSet implements ResultSet {

    private final ResultSetMetadata metadata;

    private final RelationCursor cursor;

    /**
     * Creates a new instance.
     * @param metadata the metadata
     * @param cursor the relation cursor to delegate
     */
    public BasicResultSet(ResultSetMetadata metadata, RelationCursor cursor) {
        Objects.requireNonNull(metadata);
        Objects.requireNonNull(cursor);
        this.metadata = metadata;
        this.cursor = cursor;
    }

    @Override
    public ResultSetMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean nextRow() throws IOException, ServerException, InterruptedException {
        return cursor.nextRow();
    }

    @Override
    public boolean nextColumn() throws IOException, ServerException, InterruptedException {
        return cursor.nextColumn();
    }

    @Override
    public boolean isNull() {
        return cursor.isNull();
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        cursor.setCloseTimeout(timeout);
    }

    @Override
    public boolean fetchBooleanValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchBooleanValue();
    }

    @Override
    public int fetchInt4Value() throws IOException, ServerException, InterruptedException {
        return cursor.fetchInt4Value();
    }

    @Override
    public long fetchInt8Value() throws IOException, ServerException, InterruptedException {
        return cursor.fetchInt8Value();
    }

    @Override
    public float fetchFloat4Value() throws IOException, ServerException, InterruptedException {
        return cursor.fetchFloat4Value();
    }

    @Override
    public double fetchFloat8Value() throws IOException, ServerException, InterruptedException {
        return cursor.fetchFloat8Value();
    }

    @Override
    public BigDecimal fetchDecimalValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchDecimalValue();
    }

    @Override
    public String fetchCharacterValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchCharacterValue();
    }

    @Override
    public byte[] fetchOctetValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchOctetValue();
    }

    @Override
    public boolean[] fetchBitValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchBitValue();
    }

    @Override
    public LocalDate fetchDateValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchDateValue();
    }

    @Override
    public LocalTime fetchTimeOfDayValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchTimeOfDayValue();
    }

    @Override
    public Instant fetchTimePointValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchTimePointValue();
    }

    @Override
    public DateTimeInterval fetchDateTimeIntervalValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchDateTimeIntervalValue();
    }

    @Override
    public int beginArrayValue() throws IOException, ServerException, InterruptedException {
        return cursor.beginArrayValue();
    }

    @Override
    public void endArrayValue() throws IOException, ServerException, InterruptedException {
        cursor.endArrayValue();
    }

    @Override
    public int beginRowValue() throws IOException, ServerException, InterruptedException {
        return cursor.beginRowValue();
    }

    @Override
    public void endRowValue() throws IOException, ServerException, InterruptedException {
        cursor.endRowValue();
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        cursor.close();
    }
}
