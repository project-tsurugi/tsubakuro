package com.nautilus_technologies.tsubakuro.low.sql;

import java.io.Closeable;
import java.io.IOException;

import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * ResultSet type.
 */
public interface ResultSet extends Closeable {
    /**
     * Provides record metadata holding information about field type and nullability
     */
    public interface RecordMeta {
        /**
         * Get the field type
         * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
         * @return field type
         * @throws IOException error occurred in investigating the column type
         */
        SqlCommon.AtomType type(int index) throws IOException;

        /**
         * Get the field name
         * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
         * @return field name
         * @throws IOException error occurred in investigating the column type
         */
        String name(int index) throws IOException;

        /**
         * Get the nullability for the field
         * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
         * @return true if the field is nullable
         * @throws IOException error occurred in the column checking
         */
        boolean nullable(int index) throws IOException;

        /**
         * Get the number of fields in the record
         * @return the number of the fields
         */
        int fieldCount();

        /**
         * Get the field type
         * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
         * @return field type
         * @throws IOException error occurred in investigating the column type
         */
        @Deprecated
        SqlCommon.AtomType at(int index) throws IOException;

        /**
         * Get the current field type
         * @return current field type
         * @throws IOException error occurred in investigating the column type
         */
        @Deprecated
        SqlCommon.AtomType at() throws IOException;

        /**
         * Get the nullability for the field
         * @return true if the current field is nullable
         * @throws IOException error occurred in the column checking
         */
        @Deprecated
        boolean nullable() throws IOException;
    }

    /**
     * Get the record mata data of the ResultSet
     * @return RecordMeta subclass belonging to this class
     */
    RecordMeta getRecordMeta();

    /**
     * Move the current pointer to the next record
     * @return true if the next record exists
     * @throws InterruptedException timeout occurred in record receive
     * @throws IOException error occurred in record receive
     */
    boolean nextRecord() throws InterruptedException, IOException;

    /**
     * Check whether the current column is null or not
     * @return true if the current column is null
     * @throws IOException error occurred in column move
     */
    boolean isNull() throws IOException;

    /**
     * Get the current column value
     * @return the value of the current column
     * @throws IOException error occurred in retrieving column data
     */
    int getInt4() throws IOException;
    long getInt8() throws IOException;
    float getFloat4() throws IOException;
    double getFloat8() throws IOException;
    String getCharacter() throws IOException;

    /**
     * Proceed the currnet column position
     * @return true if the next column exists
     */
    boolean nextColumn();

    /**
     * Get the current field type
     * @return current field type
     * @throws IOException error occurred in investigating column data type
     */
    SqlCommon.AtomType type() throws IOException;

    /**
     * Get the current field name
     * @return current field name
     * @throws IOException error occurred in investigating the column type
     */
    String name() throws IOException;

    /**
     * Get the nullability for the current field
     * @return true if the current field is nullable
     * @throws IOException error occurred in checking column data
     */
    boolean nullable() throws IOException;

    /**
     * Get a FutureResponse of the response returned from the SQL service
     * @return a FutureResponse of SqlResponse.ResultOnly indicate whether the SQL service has successfully completed processing or not
     */
    // FIXME: raise exception until close if error, instead.
    FutureResponse<SqlResponse.ResultOnly> getResponse();
}
