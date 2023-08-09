package com.tsurugidb.tsubakuro.kvs.bench;

class TxStatus {
    private long nloop;
    private long nrec;

    void addNumLoop(long v) {
        nloop += v;
    }

    void addNumRecord(long v) {
        nrec += v;
    }

    long getNumLoop() {
        return nloop;
    }

    long getNumRecord() {
        return nrec;
    }

}
