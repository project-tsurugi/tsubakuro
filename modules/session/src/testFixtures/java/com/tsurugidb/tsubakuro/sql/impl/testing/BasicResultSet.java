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
package com.tsurugidb.tsubakuro.sql.impl.testing;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.util.Objects;

//import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;
import com.tsurugidb.tsubakuro.sql.RelationCursor;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.ResultSetMetadata;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;
import com.tsurugidb.tsubakuro.util.Timeout;

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
    public LocalDateTime fetchTimePointValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchTimePointValue();
    }

    @Override
    public OffsetTime fetchTimeOfDayWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchTimeOfDayWithTimeZoneValue();
    }

    @Override
    public OffsetDateTime fetchTimePointWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
        return cursor.fetchTimePointWithTimeZoneValue();
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
    public BlobReference fetchBlob() throws IOException, ServerException, InterruptedException {
        return cursor.fetchBlob();
    }

    @Override
    public ClobReference fetchClob() throws IOException, ServerException, InterruptedException {
        return cursor.fetchClob();
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        cursor.close();
    }
}
