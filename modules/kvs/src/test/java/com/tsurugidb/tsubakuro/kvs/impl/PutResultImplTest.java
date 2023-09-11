package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PutResultImplTest {

    @Test
    void negativeSize() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> {
            new PutResultImpl(-1);
        });
    }

    @Test
    void sizeCheck() throws Exception {
        int[] sizes = {0, 1, 2, 10};
        for (var n : sizes) {
            var result = new PutResultImpl(n);
            assertEquals(n, result.size());
        }
    }
}
