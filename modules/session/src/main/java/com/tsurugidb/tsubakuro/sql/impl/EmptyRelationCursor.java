/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.sql.impl;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;

import com.tsurugidb.tsubakuro.sql.RelationCursor;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;
import com.tsurugidb.tsubakuro.sql.io.ValueInput;

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
    public LocalDateTime fetchTimePointValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public OffsetTime fetchTimeOfDayWithTimeZoneValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public OffsetDateTime fetchTimePointWithTimeZoneValue() throws UnsupportedOperationException {
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
