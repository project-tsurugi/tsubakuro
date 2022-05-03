package com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.net.URI;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

class TestingConnector implements Connector {

    final URI endpoint;

    TestingConnector(URI endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public FutureResponse<SessionWire> connect(Credential credential) throws IOException {
        throw new UnsupportedOperationException();
    }
}
