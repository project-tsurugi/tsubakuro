package com.tsurugidb.tsubakuro.kvs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class KvsServiceExceptionTest {

    @Test
    void onlyCode() throws Exception {
        final var code = KvsServiceCode.NOT_FOUND;
        var ex = new KvsServiceException(code);
        var str = ex.toString();
        assertEquals(true, str.contains(ex.getClass().getSimpleName()));
        assertEquals(true, str.contains(code.getStructuredCode()));
        assertEquals(true, str.contains(code.name()));
    }

    @Test
    void codeWithMessage() throws Exception {
        final var code = KvsServiceCode.NOT_FOUND;
        final var msg = "key1=abc not found";
        var ex = new KvsServiceException(code, msg);
        var str = ex.toString();
        System.out.println(str);
        assertEquals(true, str.contains(ex.getClass().getSimpleName()));
        assertEquals(true, str.contains(code.getStructuredCode()));
        // assertEquals(true, str.contains(code.name()));
        assertEquals(true, str.contains(msg));
    }

    @Test
    void codeWithCause() throws Exception {
        final var code = KvsServiceCode.NOT_FOUND;
        final var causeMessage = "caused by abcdefg";
        final var cause = new Throwable(causeMessage);
        var ex = new KvsServiceException(code, cause);
        var str = ex.toString();
        System.out.println(str);
        assertEquals(true, str.contains(ex.getClass().getSimpleName()));
        assertEquals(true, str.contains(code.getStructuredCode()));
        assertEquals(cause, ex.getCause());
    }

    @Test
    void fillArgs() throws Exception {
        final var code = KvsServiceCode.NOT_FOUND;
        final var msg = "key1=abc not found";
        final var causeMessage = "caused by abcdefg";
        final var cause = new Throwable(causeMessage);
        var ex = new KvsServiceException(code, msg, cause);
        var str = ex.toString();
        System.out.println(str);
        assertEquals(true, str.contains(ex.getClass().getSimpleName()));
        assertEquals(true, str.contains(code.getStructuredCode()));
        assertEquals(true, str.contains(msg));
        assertEquals(cause, ex.getCause());
    }
}
