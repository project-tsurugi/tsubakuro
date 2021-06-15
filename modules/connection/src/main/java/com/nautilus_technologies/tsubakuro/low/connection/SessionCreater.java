package com.nautilus_technologies.tsubakuro.low.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Session;

/**
 * SessionCreater type.
 */
public interface SessionCreater {
    /**
     * Connect to the sql service
     * @param name the database name to connect
     * @return the session
     */
    Future<Session> createSession(Connector connector) throws IOException;
}
