package com.tsurugidb.tsubakuro.common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.mock.MockWire;

class SessionBuilderTest {

    @Test
    void create() throws Exception {
        try (var wire = new MockWire()) {
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(Credential credential) throws IOException {
                    assertSame(credential, creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withCredential(creds);
            try (var session = builder.create()) {
                // ok.
            }
        }
    }

    @Test
    void createAsync() throws Exception {
        try (var wire = new MockWire()) {
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(Credential credential) throws IOException {
                    assertSame(credential, creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withCredential(creds);
            try (
                    var fSession = builder.createAsync();
                    var session = fSession.get(10, TimeUnit.SECONDS)) {
                // ok.
            }
        }
    }

}
