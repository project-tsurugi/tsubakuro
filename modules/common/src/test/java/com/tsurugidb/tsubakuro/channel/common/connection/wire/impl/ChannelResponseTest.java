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
package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.diagnostics.proto.Diagnostics;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;

class ChannelResponseTest {

    @Test
    void wrapAndThrow() throws Exception {
        try (var target = new ChannelResponse(null)) {
            {
                var e0 = new CoreServiceException(CoreServiceCode.SYSTEM_ERROR, "test");
                var e = assertThrows(ServerException.class, () -> {
                    target.wrapAndThrow(e0);
                });
                assertEquals(e0.getClass(), e.getClass());
                assertEquals(e0.getDiagnosticCode(), e.getDiagnosticCode());
                assertEquals(e0.getMessage(), e.getMessage());
                assertSame(e0, e.getCause());
            }
            {
                var e0 = new TestServerException(CoreServiceCode.SYSTEM_ERROR, "test");
                var e = assertThrows(CoreServiceException.class, () -> {
                    target.wrapAndThrow(e0);
                });
                assertEquals(e0.getMessage(), e.getMessage());
                assertSame(e0, e.getCause());
            }
            {
                var e0 = new TimeoutException("test");
                var e = assertThrows(IOException.class, () -> {
                    target.wrapAndThrow(e0);
                });
                assertEquals(ResponseTimeoutException.class, e.getClass());
                assertEquals(e0.getMessage(), e.getMessage());
                assertSame(e0, e.getCause());
            }
            {
                var e0 = new IOException("test");
                target.wrapAndThrow(e0); // do nothing
            }
        }
    }

    @SuppressWarnings("serial")
    private static class TestServerException extends CoreServiceException {

        private final CoreServiceCode code;

        public TestServerException(CoreServiceCode code, String message) {
            super(CoreServiceCode.valueOf(Diagnostics.Code.SYSTEM_ERROR), message);
            this.code = code;
        }

        @Override
        public CoreServiceCode getDiagnosticCode() {
            return this.code;
        }
    }
}
