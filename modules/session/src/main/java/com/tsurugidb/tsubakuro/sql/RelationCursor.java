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
package com.tsurugidb.tsubakuro.sql;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;

import javax.annotation.concurrent.NotThreadSafe;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * A cursor for retrieving contents in relations.
 */
@NotThreadSafe
public interface RelationCursor extends ServerResource {

    /**
     * Advances the cursor to the head of the next row.
     * <p>
     * If this operation was succeeded (returns {@code true}), this cursor points the head of the next row.
     * After this operation, you need to invoke {@link #nextColumn()} to retrieve the first column data of the next row.
     * </p>
     * @return {@code true} if the cursor successfully advanced to the head of the next row,
     *  or {@code false} if there are no more rows in this relation.
     * @throws IOException if I/O error was occurred while retrieving the next row data
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while retrieving the next row data
     */
    boolean nextRow() throws IOException, ServerException, InterruptedException;

    /**
     * Advances the cursor to the next column in the current row.
     * <p>
     * If this operation was succeeded (returns {@code true}), this cursor will point to the next column of the row.
     * You can invoke {@code fetchXxx()} method to obtain the column value, or
     * </p>
     * @return {@code true} if the cursor successfully advanced to the head of the next row,
     *  or {@code false} if there are no more rows in this relation.
     * @throws IOException if I/O error was occurred while retrieving the next column data
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while retrieving the next column data
     */
    boolean nextColumn() throws IOException, ServerException, InterruptedException;

    /**
     * Returns whether or not the column on this cursor is {@code NULL}.
     * @return {@code true} if the column is {@code NULL}, or {@code false} otherwise
     * @throws IllegalStateException if this cursor does not point to any columns
     */
    boolean isNull();

    /**
     * Retrieves a {@code BOOLEAN} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#BOOLEAN
     */
    boolean fetchBooleanValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code INT4} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#INT4
     */
    int fetchInt4Value() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code INT8} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#INT8
     */
    long fetchInt8Value() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code FLOAT4} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#FLOAT4
     */
    float fetchFloat4Value() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code FLOAT8} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#FLOAT8
     */
    double fetchFloat8Value() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code DECIMAL} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#FLOAT8
     */
    BigDecimal fetchDecimalValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code CHARACTER} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#CHARACTER
     */
    String fetchCharacterValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code OCTET} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#OCTET
     */
    byte[] fetchOctetValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code BIT} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#BIT
     */
    boolean[] fetchBitValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code DATE} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#DATE
     */
    LocalDate fetchDateValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code TIME_OF_DAY} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#TIME_OF_DAY
     */
    LocalTime fetchTimeOfDayValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code TIME_POINT} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * <p>
     * Note that, this does not include time-zone information.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#TIME_POINT
     */
    LocalDateTime fetchTimePointValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code TIME_OF_DAY_WITH_TIME_ZONE} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#TIME_OF_DAY_WITH_TIME_ZONE
     */
    OffsetTime fetchTimeOfDayWithTimeZoneValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code TIME_POINT_WITH_TIME_ZONE} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * <p>
     * Note that, this does not include time-zone information.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#TIME_POINT_WITH_TIME_ZONE
     */
    OffsetDateTime fetchTimePointWithTimeZoneValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves a {@code DATETIME_INTERVAL} value on the column of the cursor position.
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the value
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see com.tsurugidb.sql.proto.SqlCommon.AtomType#DATETIME_INTERVAL
     */
    DateTimeInterval fetchDateTimeIntervalValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves beginning of {@code ARRAY} value on the column of the cursor position.
     * <p>
     * This does not fetch elements of the array, please retrieve individual elements as {@link #nextColumn() column)}.
     * After retrieving elements, you must invoke {@link #endArrayValue()} to finish this array.
     * </p>
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * @return the number of elements in the array
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     */
    int beginArrayValue() throws IOException, ServerException, InterruptedException;

    /**
     * Exits from current {@link #beginArrayValue() array value}.
     * <p>
     * If there are some rest elements in the array, this operation will discards them.
     * </p>
     * @throws IllegalStateException if the current structure is not an array (e.g. enter to rows)
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     */
    void endArrayValue() throws IOException, ServerException, InterruptedException;

    /**
     * Retrieves beginning of {@code ROW} value on the column of the cursor position.
     * <p>
     * This does not fetch elements of the row, please retrieve individual elements as {@link #nextColumn() column)}.
     * After retrieving elements, you must invoke {@link #endArrayValue()} to finish this array.
     * </p>
     * <p>
     * You can only take once to retrieve the value on the column.
     * </p>
     * <p>
     * Note that, this is only available for row values in column.
     * For top-level row in the relation, please use {@link #nextRow()} instead.
     * </p>
     *
     * @return the number of elements in the row
     * @throws IllegalStateException if the value has been already fetched
     * @throws IllegalStateException if this cursor does not point to any columns
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws IOException if the value type is not matched
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     * @see #nextRow()
     */
    int beginRowValue() throws IOException, ServerException, InterruptedException;

    /**
     * Exits from current {@link #beginRowValue() row value}.
     * <p>
     * If there are some rest elements in the array, this operation will discards them.
     * </p>
     * <p>
     * Note that, this is only available for row values started in {@link #beginRowValue()}.
     * To end top-level rows in the relation, please use {@link #nextRow()} instead.
     * </p>
     *
     * @throws IllegalStateException if the current structure is not a row (e.g. enter to arrays)
     * @throws IOException if I/O error was occurred while extracting the column data
     * @throws ServerException if server error was occurred while retrieving this relation
     * @throws InterruptedException if interrupted while extracting the column data
     */
    void endRowValue() throws IOException, ServerException, InterruptedException;

    // FIXME impl clob, blob
}
