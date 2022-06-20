package com.nautilus_technologies.tsubakuro.low.sql.io;

/**
 * Represents a data type.
 */
public enum EntryType {

    /**
     * Pseudo data type of end of relation mark.
     */
    END_OF_CONTENTS,

    /**
     * Represents value is absent.
     */
    NULL,

    /**
     * 64-bit signed integer.
     * This represents {@code INT4}, {@code INT8} or {@code BOOLEAN}.
     */
    INT,

    /**
     * Fixed 32-bit floating point number.
     */
    FLOAT4,

    /**
     * Fixed 64-bit floating point number.
     */
    FLOAT8,

    /**
     * Fixed 128-bit floating point decimal.
     */
    DECIMAL,

    /**
     * Variable length character sequence.
     */
    CHARACTER,

    /**
     * Variable length octet sequence.
     */
    OCTET,

    /**
     * Variable length bit sequence.
     */
    BIT,

    /**
     * Date value.
     */
    DATE,

    /**
     * Time of day value.
     */
    TIME_OF_DAY,

    /**
     * Time-point value.
     */
    TIME_POINT,

    /**
     * Date-time interval value.
     */
    DATETIME_INTERVAL,

    /**
     * Rows.
     */
    ROW,

    /**
     * Arrays.
     */
    ARRAY,

    /**
     * Character large objects.
     */
    CLOB,

    /**
     * Binary large objects.
     */
    BLOB,
}
