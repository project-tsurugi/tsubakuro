package com.tsurugidb.tsubakuro.kvs.ycsb;

/**
 * constant values for benchmark
 */
public interface Constants {

    /**
     * table name
     */
    String TABLE_NAME = "YCSBbench";

    /**
     * the name of the primary key column
     */
    String KEY_NAME = "pk";

    /**
     * the name of the value column
     */
    String VALUE_NAME = "v1";

    /**
     * the SQL type name of primary key
     */
    String KEY_TYPE = "BIGINT";

    /**
     * length of the primary key [byte]
     */
    int KEY_SIZE = 8;

    /**
     * the SQL type name of value
     */
    String VALUE_TYPE = "BIGINT";

    /**
     * length of the value [byte]
     */
    int VALUE_SIZE = 8;

    /**
     * number of operations in one transaction
     */
    int OPS_PER_TX = 10;

    /**
     * number of records in the table
     */
    int NUM_RECORDS = 100_000;

}
