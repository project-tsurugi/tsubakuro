package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.CommitType;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.PutType;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class CommitTest extends TestBase {

    private static final String TABLE_NAME = "table" + CommitTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public CommitTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    @Test
    public void unspecified() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, 1L);
                buffer.add(VALUE_NAME, 100L);
                kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                kvs.commit(tx, CommitType.UNSPECIFIED).await();
            }
        }
    }

    @Test
    public void multiCommit() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, 1L);
                buffer.add(VALUE_NAME, 100L);
                kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                kvs.commit(tx, CommitType.UNSPECIFIED).await();
                // the tx is already disposed because commit was called with autoDispose=true always
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.commit(tx, CommitType.UNSPECIFIED).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
            }
        }
    }

    @Test
    public void opsAfterCommit() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                RecordBuffer buffer = new RecordBuffer();
                buffer.add(KEY_NAME, 1L);
                buffer.add(VALUE_NAME, 100L);
                kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                kvs.commit(tx, CommitType.UNSPECIFIED).await();

                // the tx is already disposed because commit was called with autoDispose=true always
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                //
                buffer.clear();
                buffer.add(KEY_NAME, 1L);
                ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.get(tx, TABLE_NAME, buffer).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                //
                ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.remove(tx, TABLE_NAME, buffer).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
                //
                ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.rollback(tx).await();
                });
                assertEquals(KvsServiceCode.INVALID_ARGUMENT, ex.getDiagnosticCode());
            }
        }
    }

    @Test
    public void commitTypes() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            for (var cmtType : CommitType.values()) {
                if (cmtType == CommitType.UNSPECIFIED) {
                    continue;
                }
                try (var tx = kvs.beginTransaction().await()) {
                    RecordBuffer buffer = new RecordBuffer();
                    buffer.add(KEY_NAME, 1L);
                    buffer.add(VALUE_NAME, 100L);
                    kvs.put(tx, TABLE_NAME, buffer, PutType.OVERWRITE).await();
                    KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                        kvs.commit(tx, cmtType).await();
                    });
                    assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
                    System.err.println(ex.getMessage());
                    // rollback and dispose will be called at tx.close()
                }
            }
        }
    }

}
