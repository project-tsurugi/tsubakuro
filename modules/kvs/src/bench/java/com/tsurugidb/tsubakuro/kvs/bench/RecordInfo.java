package com.tsurugidb.tsubakuro.kvs.bench;

class RecordInfo {
    private final ValueType type;
    private final int num;

    RecordInfo(ValueType type, int num) {
        this.type = type;
        this.num = num;
    }

    ValueType type() {
        return type;
    }

    int num() {
        return num;
    }

    @Override
    public String toString() {
        return type.toString() + ":" + num;
    }
}
