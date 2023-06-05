package com.tsurugidb.tsubakuro.kvs.bench;

/**
 * Information of what kind of record should be made.
 */
public class RecordInfo {
    private final ValueType type;
    private final int num;

    /**
     * Creates a new instance.
     * @param type the type of the value
     * @param num the number of the values (except the key column)
     */
    public RecordInfo(ValueType type, int num) {
        this.type = type;
        this.num = num;
    }

    /**
     * retrieves the type of the value.
     * @return the type of the value
     */
    public ValueType type() {
        return type;
    }

    /**
     * retrieves the number of the values.
     * @return number of the values
     */
    public int num() {
        return num;
    }

    @Override
    public String toString() {
        return type.toString() + ":" + num;
    }
}
