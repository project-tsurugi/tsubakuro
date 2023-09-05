package com.tsurugidb.tsubakuro.kvs.basic;

import java.util.LinkedList;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.TransactionOption;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

public class BeginTest extends TestBase {

    private static final String TABLE_NAME = "table" + BeginTest.class.getSimpleName();
    private static final String KEY_NAME = "k1";
    private static final String VALUE_NAME = "v1";

    public BeginTest() throws Exception {
        String schema = String.format("%s BIGINT PRIMARY KEY, %s BIGINT", KEY_NAME, VALUE_NAME);
        createTable(TABLE_NAME, schema);
    }

    @Test
    public void transactionTypes() throws Exception {
        var options = new LinkedList<TransactionOption>();
        options.add(new TransactionOption());
        options.add(TransactionOption.forShortTransaction().build());
        options.add(TransactionOption.forLongTransaction().build());
        options.add(TransactionOption.forReadOnlyTransaction().build());
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            for (var opt : options) {
                try (var tx = kvs.beginTransaction(opt).await()) {
                    kvs.rollback(tx);
                }
            }
        }
    }

    // TODO write preserves etc.
}
