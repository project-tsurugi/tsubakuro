package com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;

class TestingConnector implements Connector {

    final URI endpoint;

    TestingConnector(URI endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public Future<SessionWire> connect(Credential credential) throws IOException {
        throw new UnsupportedOperationException();
    }
}
