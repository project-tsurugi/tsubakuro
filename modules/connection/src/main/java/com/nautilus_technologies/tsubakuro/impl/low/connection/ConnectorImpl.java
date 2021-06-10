package com.nautilus_technologies.tsubakuro.impl.low.connection;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;

/**
 * Connector type.
 */
public class ConnectorImpl implements Connector {
    /**
     * Request executeStatement to the SQL service
     * @param name the database name to connect
     * @return Future<Session> the session
     */
    public Future<Session> connect(String name) throws IOException {
	return new FutureSessionImpl(IpcConnectorImpl.connect(name));
    }

    /**
     * Close the Connector
     */
    public void close() throws IOException {  // FIXME
    }
}
