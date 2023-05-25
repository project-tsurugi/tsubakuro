package com.tsurugidb.tsubakuro.kvs.bench;

class TxStatus {
    private long nloop;
    private long nrec;
    private long msec;

    void addNumLoop(long v) {
        nloop += v;
    }

    void addNumRecord(long v) {
        nrec += v;
    }

    void setElapseMsec(long v) {
        this.msec = v;
    }

    void addElapseMsec(long v) {
        this.msec += v;
    }

    long getNumLoop() {
        return nloop;
    }

    long getNumRecord() {
        return nrec;
    }

    long getElapseMsec() {
        return msec;
    }
}
