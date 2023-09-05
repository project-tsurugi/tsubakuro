package com.tsurugidb.tsubakuro.kvs.ycsb;

/**
 * constant values for benchmark
 */
public final class Constants {

    /**
     * table name
     */
    public static final String TABLE_NAME = "YCSBbench";

    /**
     * whether every sessions use the same table or not
     */
    public static final boolean USE_SAME_TABLE = true;

    /**
     * the name of the primary key column
     */
    public static final String KEY_NAME = "pk";

    /**
     * the name of the value column
     */
    public static final String VALUE_NAME = "v1";

    /**
     * the SQL type name of primary key
     */
    public static final String KEY_TYPE = "BIGINT";

    /**
     * length of the primary key [byte]
     */
    public static final int KEY_SIZE = 8;

    /**
     * the SQL type name of value
     */
    public static final String VALUE_TYPE = "BIGINT";

    /**
     * length of the value [byte]
     */
    public static final int VALUE_SIZE = 8;

    /**
     * number of operations in one transaction
     */
    public static final int OPS_PER_TX = 10;

    /**
     * number of records in the table
     */
    public static final int NUM_RECORDS = 100_000;

    private Constants() {
        throw new AssertionError();
    }
}
