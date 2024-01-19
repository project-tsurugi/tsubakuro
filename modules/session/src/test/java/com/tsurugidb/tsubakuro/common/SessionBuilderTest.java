package com.tsurugidb.tsubakuro.common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.impl.MockWire;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

class SessionBuilderTest {

    @Test
    void create() throws Exception {
        try (var wire = new MockWire()) {
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getCredential(), creds);
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
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getCredential(), creds);
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

    @Test
    void createWithClientInformation() throws Exception {
        try (var wire = new MockWire()) {
            String label = "label for the test";
            String applicationName = "applicationName for the test";
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getConnectionLabel(), label);
                    assertSame(clientInformation.getApplicationName(), applicationName);
                    assertSame(clientInformation.getCredential(), creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withLabel(label).withApplicationName(applicationName).withCredential(creds);
            try (var session = builder.create()) {
                // ok.
            }
        }
    }

    @Test
    void createAsyncWithClientInformation() throws Exception {
        try (var wire = new MockWire()) {
            String label = "label for the test";
            String applicationName = "applicationName for the test";
            var creds = new RememberMeCredential("testing");
            var builder = SessionBuilder.connect(new Connector() {
                @Override
                public FutureResponse<Wire> connect(ClientInformation clientInformation) throws IOException {
                    assertSame(clientInformation.getConnectionLabel(), label);
                    assertSame(clientInformation.getApplicationName(), applicationName);
                    assertSame(clientInformation.getCredential(), creds);
                    return FutureResponse.wrap(Owner.of(wire));
                }
            })
                    .withLabel(label).withApplicationName(applicationName).withCredential(creds);
            try (
                    var fSession = builder.createAsync();
                    var session = fSession.get(10, TimeUnit.SECONDS)) {
                // ok.
            }
        }
    }

}
