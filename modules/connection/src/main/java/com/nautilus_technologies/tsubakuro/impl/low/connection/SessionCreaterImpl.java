package com.nautilus_technologies.tsubakuro.impl.low.connection;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.connection.SessionCreater;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;
import com.nautilus_technologies.tsubakuro.low.sql.Session;

/**
 * SessionCreater type.
 */
public class SessionCreaterImpl implements SessionCreater {
    /**
     * Request executeStatement to the SQL service
     * @param name the database name to connect
     * @return Future<Session> the session
     */
    public Future<Session> createSession(Connector connector) throws IOException {
	return new FutureSessionImpl(connector.connect());
    }
}
