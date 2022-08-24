package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import com.nautilus_technologies.tsubakuro.low.sql.RelationCursor;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.nautilus_technologies.tsubakuro.low.sql.io.ValueInput;

/**
 * An implementation of {@link RelationCursor}, which retrieves relation data from the {@link ValueInput}.
 */
public class EmptyRelationCursor implements RelationCursor {
    /**
     * Creates a new instance.
     */
    public EmptyRelationCursor() {
    }

    @Override
    public boolean nextRow() {
        return false;
    }

    @Override
    public boolean nextColumn() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean fetchBooleanValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int fetchInt4Value() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long fetchInt8Value() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public float fetchFloat4Value() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public double fetchFloat8Value() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal fetchDecimalValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String fetchCharacterValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] fetchOctetValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] fetchBitValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDate fetchDateValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalTime fetchTimeOfDayValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant fetchTimePointValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DateTimeInterval fetchDateTimeIntervalValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int beginArrayValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void endArrayValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int beginRowValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void endRowValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // do nothing
    }
}
