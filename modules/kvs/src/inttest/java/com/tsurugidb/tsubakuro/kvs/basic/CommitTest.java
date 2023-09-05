package com.tsurugidb.tsubakuro.kvs.basic;

import java.util.LinkedList;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.CommitType;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

public class CommitTest extends TestBase {

    private static final String TABLE_NAME = "table" + CommitTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public CommitTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    @Test
    public void commitTypes() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            for (var cmtType : CommitType.values()) {
                try (var tx = kvs.beginTransaction().await()) {
                    RecordBuffer buffer = new RecordBuffer();
                    buffer.add(KEY_NAME, 1L);
                    buffer.add(VALUE_NAME, 100L);
                    kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                    kvs.commit(tx, cmtType).await();
                }
            }
        }
    }

}
