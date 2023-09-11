package com.tsurugidb.tsubakuro.kvs.basic;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;
import com.tsurugidb.tsubakuro.kvs.ScanBound;
import com.tsurugidb.tsubakuro.kvs.ScanType;
import com.tsurugidb.tsubakuro.kvs.util.TestBase;

class ScanTest extends TestBase {

    private static final String TABLE_NAME = "table" + ScanTest.class.getSimpleName();

    @Test
    public void notSupportedYet() throws Exception {
        RecordBuffer lowerKey = new RecordBuffer();
        ScanBound lowerBound = ScanBound.INCLUSIVE;
        RecordBuffer upperKey = new RecordBuffer();
        ScanBound upperBound = ScanBound.INCLUSIVE;
        ScanType behavior = ScanType.FORWARD;
        try (var session = getNewSession(); var kvs = KvsClient.attach(session)) {
            try (var tx = kvs.beginTransaction().await()) {
                assertThrows(UnsupportedOperationException.class,
                        () -> kvs.scan(tx, TABLE_NAME, lowerKey, lowerBound, upperKey, upperBound, behavior).await());
                kvs.rollback(tx).await();
            }
        }
    }
}
