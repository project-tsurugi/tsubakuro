package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import javax.annotation.concurrent.NotThreadSafe;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#BOOLEAN
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#INT4
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#INT8
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#FLOAT4
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#FLOAT8
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#FLOAT8
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#CHARACTER
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#OCTET
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#BIT
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#DATE
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#TIME_OF_DAY
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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#TIME_POINT
     */
    Instant fetchTimePointValue() throws IOException, ServerException, InterruptedException;

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
     * @see com.tsurugidb.jogasaki.proto.SqlCommon.AtomType#DATETIME_INTERVAL
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


    // for compatibility
    /**
     * Advances the cursor to the head of the next row.
     * @deprecated Method name changed to nextRow
     */
    @Deprecated
    default boolean nextRecord() throws IOException, ServerException, InterruptedException {
        return nextRow();
    }
    /**
     * Retrieves a {@code INT4} value on the column of the cursor position.
     * @deprecated Method name changed to fetchInt4Value
     */
    @Deprecated
    default int getInt4() throws IOException, ServerException, InterruptedException {
        return fetchInt4Value();
    }
    /**
     * Retrieves a {@code INT8} value on the column of the cursor position.
     * @deprecated Method name changed to fetchInt8Value
     */
    @Deprecated
    default long getInt8() throws IOException, ServerException, InterruptedException {
        return fetchInt8Value();
    }
    /**
     * Retrieves a {@code FLOAT4} value on the column of the cursor position.
     * @deprecated Method name changed to fetchFloat4Value
     */
    @Deprecated
    default float getFloat4() throws IOException, ServerException, InterruptedException {
        return fetchFloat4Value();
    }
    /**
     * Retrieves a {@code FLOAT8} value on the column of the cursor position.
     * @deprecated Method name changed to fetchFloat8Value
     */
    @Deprecated
    default double getFloat8() throws IOException, ServerException, InterruptedException {
        return fetchFloat8Value();
    }
    /**
     * Retrieves a {@code CHARACTER} value on the column of the cursor position.
     * @deprecated Method name changed to fetchCharacterValue
     */
    @Deprecated
    default String getCharacter() throws IOException, ServerException, InterruptedException {
        return fetchCharacterValue();
    }
}
