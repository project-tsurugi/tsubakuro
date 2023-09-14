package com.tsurugidb.tsubakuro.kvs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

class RecordBufferTest {

    @Test
    void empty() throws Exception {
        var buffer = new RecordBuffer();
        assertEquals(0, buffer.size());
        var record = buffer.toRecord();
        assertEquals(0, record.size());
    }

    private static final String KEY1 = "key1";

    @Test
    void single() throws Exception {
        var buffer = new RecordBuffer();
        buffer.add(KEY1, "hello");
        assertEquals(1, buffer.size());
        var record = buffer.toRecord();
        assertEquals(1, record.size());
    }

    @Test
    void singleNull() throws Exception {
        var buffer = new RecordBuffer();
        buffer.addNull(KEY1);
        assertEquals(1, buffer.size());
        var record = buffer.toRecord();
        assertEquals(1, record.size());
    }

    @Test
    void multi() throws Exception {
        var buffer = new RecordBuffer();
        buffer.add("key-1", 123);
        buffer.add("key-2", 456.4);
        buffer.add("key-3", BigDecimal.valueOf(1234));
        buffer.add("key-4", "hello");
        assertEquals(4, buffer.size());
        var record = buffer.toRecord();
        assertEquals(4, record.size());
    }

    @Test
    void duplicateKey() throws Exception {
        var buffer = new RecordBuffer();
        buffer.add("key", 123);
        buffer.add("key", 456.4);
        assertEquals(2, buffer.size());
        var record = buffer.toRecord();
        assertEquals(2, record.size());
    }

    @Test
    void clear() throws Exception {
        var buffer = new RecordBuffer();
        buffer.add("key", 123);
        assertEquals(1, buffer.size());
        assertEquals(1, buffer.toRecord().size());
        buffer.clear();
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.toRecord().size());
    }

}
