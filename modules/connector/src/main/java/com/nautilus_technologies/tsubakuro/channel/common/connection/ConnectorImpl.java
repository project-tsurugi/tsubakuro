package com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.stream.connection.StreamConnectorImpl;
import com.nautilus_technologies.tsubakuro.channel.ipc.connection.IpcConnectorImpl;

/**
 * ConnectorImpl type.
 */
public final class ConnectorImpl implements Connector {
    private Connector connector;

    public ConnectorImpl(String name) {
        if (name.contains(":")) {
            String[] elements = name.split(":");

            int port;
            if (elements.length < 2) {
                port = StreamConnectorImpl.DEFAULT_PORT;
            } else {
                if (elements.length > 2) {
                    System.err.println("`:` shoud be one in the connection name");
                }
                try {
                    port = Integer.parseInt(elements[1]);
                } catch (NumberFormatException e) {
                    System.err.println(elements[1] + " is not a number, use default port ("
                            + StreamConnectorImpl.DEFAULT_PORT + ")");
                    port = StreamConnectorImpl.DEFAULT_PORT;
                }
            }
            this.connector = new StreamConnectorImpl(elements[0], port);
        } else {
            this.connector = new IpcConnectorImpl(name);
        }
    }

    @Override
    public Future<SessionWire> connect(Credential credential) throws IOException {
        return connector.connect(credential);
    }
}
