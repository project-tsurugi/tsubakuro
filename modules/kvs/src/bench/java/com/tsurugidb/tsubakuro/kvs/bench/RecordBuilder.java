package com.tsurugidb.tsubakuro.kvs.bench;

import java.math.BigDecimal;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;

/**
 * Dummy record object builder.
 */
public class RecordBuilder {

    private final RecordInfo info;
    private long v = System.nanoTime();

    /**
     * first primary key name.
     */
    public static final String FIRST_KEY_NAME = "key1";

    /**
     * value name prefix.
     */
    public static final String VALUE_NAME_PREFIX = "value";

    /**
     * the value of first column index.
     */
    public static final int FIRST_COUMN_INDEX = 1;

    /**
     * the name of first column.
     */
    public static final String FIRST_VALUE_NAME = VALUE_NAME_PREFIX + FIRST_COUMN_INDEX;

    /**
     * Creates a new instance.
     * @param info what kind of record should be made
     */
    public RecordBuilder(RecordInfo info) {
        this.info = info;
    }

    /**
     * Creates a new dummy record.
     * @return a dummy record
     */
    public RecordBuffer makeRecordBuffer() {
        RecordBuffer buffer = new RecordBuffer();
        buffer.add(FIRST_KEY_NAME, Long.valueOf(v++));
        for (int i = FIRST_COUMN_INDEX; i < FIRST_COUMN_INDEX + info.num(); i++, v++) {
            final var name = VALUE_NAME_PREFIX + i;
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
