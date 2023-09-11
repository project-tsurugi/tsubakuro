package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.tsurugidb.kvs.proto.KvsTransaction.Priority;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class BeginTest extends TestBase {

    @Test
    public void occ() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                kvs.rollback(tx);
            }
            try (var tx = kvs.beginTransaction(TransactionOption.forShortTransaction().build()).await()) {
                kvs.rollback(tx);
            }
            try (var tx = kvs.beginTransaction(TransactionOption.forShortTransaction().withPriority(Priority.PRIORITY_UNSPECIFIED).build()).await()) {
                kvs.rollback(tx);
            }
        }
    }

    @Test
    public void occWithArgs() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            {
                var opt = TransactionOption.forShortTransaction().withLabel("abc").build();
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(opt).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
            {
                for (var p : Priority.values()) {
                    if (p == Priority.PRIORITY_UNSPECIFIED || p == Priority.UNRECOGNIZED) {
                        continue;
                    }
                    var opt = TransactionOption.forShortTransaction().withPriority(p).build();
                    KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                        kvs.beginTransaction(opt).await();
                    });
                    assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
                }
            }
        }
    }

    @Test
    public void ltxWithArgs() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            {
                var opt = TransactionOption.forLongTransaction().withLabel("abc").build();
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(opt).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
            {
                for (var p : Priority.values()) {
                    if (p == Priority.PRIORITY_UNSPECIFIED || p == Priority.UNRECOGNIZED) {
                        continue;
                    }
                    var opt = TransactionOption.forLongTransaction().withPriority(p).build();
                    KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                        kvs.beginTransaction(opt).await();
                    });
                    assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
                }
            }
            {
                var opt = TransactionOption.forLongTransaction().withModifyDefinitions(true).build();
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(opt).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
            {
                var opt = TransactionOption.forLongTransaction().addWritePreserve("table1").build();
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(opt).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
            {
                var opt = TransactionOption.forLongTransaction().addInclusiveReadArea("table1").build();
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(opt).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
            {
                var opt = TransactionOption.forLongTransaction().addExclusiveReadArea("table1").build();
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(opt).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
        }
    }

    @Test
    public void otherTypes() throws Exception {
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            {
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(TransactionOption.forLongTransaction().build()).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
            {
                KvsServiceException ex = assertThrows(KvsServiceException.class, () -> {
                    kvs.beginTransaction(TransactionOption.forReadOnlyTransaction().build()).await();
                });
                assertEquals(KvsServiceCode.NOT_IMPLEMENTED, ex.getDiagnosticCode());
            }
        }
    }

    // TODO write preserves etc.
}
