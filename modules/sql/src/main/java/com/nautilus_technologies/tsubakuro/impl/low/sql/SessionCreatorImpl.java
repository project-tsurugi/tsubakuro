package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionCreator;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;

/**
 * SessionCreatorImpl type.
 */
public class SessionCreatorImpl implements SessionCreator {
    /**
     * Create session
     * @param connector the connector that creates sessionWire to the Database
     * @return Future<Session> a implementation of future session
     */
    public Future<Session> createSession(Connector connector) throws IOException {
	return new FutureSessionImpl(connector.connect());
    }
}
