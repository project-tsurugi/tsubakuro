package com.tsurugidb.tsubakuro.kvs.bench;

import java.math.BigDecimal;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;

class RecordBuilder {

    private final RecordInfo info;
    private long v = System.nanoTime();

    RecordBuilder(RecordInfo type) {
        this.info = type;
    }

    RecordBuffer makeRecordBuffer() {
        RecordBuffer buffer = new RecordBuffer();
        buffer.add("key", Long.valueOf(v));
        for (int i = 0; i < info.num(); i++, v++) {
            final var name = "value" + i;
            switch (info.type()) {
            case LONG:
                buffer.add(name, Long.valueOf(v));
                break;
            case STRING:
                buffer.add(name, Long.toString(v));
                break;
            case DECIMAL:
                buffer.add(name, BigDecimal.valueOf(v));
                break;
            }
        }
        return buffer;
    }

    KvsData.Record makeKvsDataRecord() {
        return makeRecordBuffer().toRecord().getEntity();
    }
}
