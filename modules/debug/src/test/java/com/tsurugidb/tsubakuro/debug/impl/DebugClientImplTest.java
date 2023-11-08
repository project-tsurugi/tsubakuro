package com.tsurugidb.tsubakuro.debug.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.debug.proto.DebugRequest;
import com.tsurugidb.tsubakuro.debug.LogLevel;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class DebugClientImplTest {

    @Test
    void logging() throws Exception {
        var client = new DebugClientImpl(new DebugService() {
            @Override
            public FutureResponse<Void> send(DebugRequest.Logging request) throws IOException {
                assertEquals(DebugRequest.Logging.Level.NOT_SPECIFIED, request.getLevel());
                assertEquals("TESTING", request.getMessage());
                return FutureResponse.returns(null);
            }
        });
        client.logging("TESTING").await();
    }

    @Test
    void loggingWithLevel() throws Exception {
        var client = new DebugClientImpl(new DebugService() {
            @Override
            public FutureResponse<Void> send(DebugRequest.Logging request) throws IOException {
                assertEquals(DebugRequest.Logging.Level.WARN, request.getLevel());
                assertEquals("TESTING", request.getMessage());
                return FutureResponse.returns(null);
            }
        });
        client.logging(LogLevel.WARN, "TESTING").await();
    }
}
