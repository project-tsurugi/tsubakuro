package com.nautilus_technologies.tsubakuro.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.connection.Connector;

/**
 * SessionCreator type.
 */
public interface SessionCreator {
    /**
     * Create session
     * @param connector the connector that creates sessionWire
     * @return Future<Session> a future session
     */
    Future<Session> createSession(Connector connector) throws IOException;
}
