package com.tsurugidb.tsubakuro.kvs.bench;

/**
 * elapsed time counter.
 */
class Elapse {

    private final long start = System.currentTimeMillis();

    /**
     * retrieve elapsed time in milli second.
     * @return elapsed time in milli second
     */
    public long msec() {
        return System.currentTimeMillis() - start;
    }

}
