/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
