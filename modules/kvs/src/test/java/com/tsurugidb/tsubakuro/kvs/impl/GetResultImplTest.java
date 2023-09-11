package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;

public class GetResultImplTest {

    @Test
    void emptyRecord() throws Exception {
        var result = new GetResultImpl(new ArrayList<KvsData.Record>());
        assertEquals(0, result.size());

        var opt = result.asOptional();
        assertEquals(false, opt.isPresent());
        assertEquals(true, opt.isEmpty());
        assertThrows(NoSuchElementException.class, () -> {
            opt.get();
        });

        assertThrows(IllegalStateException.class, () -> {
            result.asRecord();
        });

        var list = result.asList();
        assertEquals(0, list.size());
        assertThrows(UnsupportedOperationException.class, () -> {
            list.add(null);
        });
        assertThrows(UnsupportedOperationException.class, () -> {
            list.clear();
        });
    }

    @Test
    void singleRecord() throws Exception {
        var buffer = new RecordBuffer();
        buffer.add("key1", "abc");
        buffer.add("key2", "hello");
        var record = buffer.toRecord();
        var entity = record.getEntity();
        var result = new GetResultImpl(entity);
        assertEquals(1, result.size());

        var opt = result.asOptional();
        assertEquals(true, opt.isPresent());
        assertEquals(false, opt.isEmpty());
        assertEquals(record, opt.get());
        assertEquals(record.size(), opt.get().size());

        assertEquals(record, result.asRecord());
        assertEquals(record.size(), result.asRecord().size());

        var list = result.asList();
        assertEquals(1, list.size());
        assertEquals(record, list.get(0));
        assertEquals(record.size(), list.get(0).size());
        assertThrows(UnsupportedOperationException.class, () -> {
            list.add(null);
        });
        assertThrows(UnsupportedOperationException.class, () -> {
            list.clear();
        });
    }

    @Test
    void doubleRecord() throws Exception {
        var records = new ArrayList<KvsData.Record>(2);
        {
            var buffer = new RecordBuffer();
            buffer.add("key1", "abc");
            buffer.add("key2", "hello");
            records.add(buffer.toRecord().getEntity());
        }
        {
            var buffer = new RecordBuffer();
            buffer.add("key1", "def");
            buffer.add("key2", "today");
            records.add(buffer.toRecord().getEntity());
        }
        var result = new GetResultImpl(records);
        assertEquals(2, result.size());

        assertThrows(IllegalStateException.class, () -> {
            result.asOptional();
        });
        assertThrows(IllegalStateException.class, () -> {
            result.asRecord();
        });

        var list = result.asList();
        assertEquals(2, list.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(records.get(i), list.get(i).getEntity());
        }
        assertThrows(UnsupportedOperationException.class, () -> {
            list.add(null);
        });
        assertThrows(UnsupportedOperationException.class, () -> {
            list.clear();
        });
    }
}
