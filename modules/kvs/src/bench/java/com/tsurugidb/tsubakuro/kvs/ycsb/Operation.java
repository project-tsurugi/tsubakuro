package com.tsurugidb.tsubakuro.kvs.ycsb;

/**
 * single operation of PUT ot GET with key
 */
public class Operation {

    private final boolean isGet;
    private final long key;

    /**
     * constructs the object
     * @param isGet whether this operation is GET or not
     * @param key the key to be used PUT or GET
     */
    public Operation(boolean isGet, long key) {
        this.isGet = isGet;
        this.key = key;
    }

    /**
     * @return whether this operation is GET or not
     */
    public boolean isGet() {
        return isGet;
    }

    /**
     * @return the key to be used PUT or GET
     */
    public long key() {
        return key;
    }

}
