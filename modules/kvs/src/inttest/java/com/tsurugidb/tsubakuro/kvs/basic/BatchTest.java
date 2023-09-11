package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.BatchScript;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class BatchTest extends TestBase {

    @Test
    public void notSupportedYet() throws Exception {
        BatchScript script = new BatchScript();
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                assertThrows(UnsupportedOperationException.class, () -> kvs.batch(tx, script).await());
                kvs.rollback(tx).await();
            }
        }
    }
}
