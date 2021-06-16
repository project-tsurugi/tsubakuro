package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;

/**
 * SessionCreatorImpl type.
 */
public final class SessionCreatorImpl {
    private SessionCreatorImpl() {
    }
    
    /**
     * Request executeStatement to the SQL service
     * @param name the database name to connect
     * @return Future<Session> the session
     */
    public static Future<Session> createSession(Connector connector) throws IOException {
	return new FutureSessionImpl(connector.connect());
    }
}
