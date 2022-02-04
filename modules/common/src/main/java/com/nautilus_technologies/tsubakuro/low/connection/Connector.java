package com.nautilus_technologies.tsubakuro.low.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;

/**
 * Connector type.
 */
public interface Connector {
    /**
     * Connect to the sql service
     * @param name the name of the SQL server to connect
     * @return the session
     */
    Future<SessionWire> connect() throws IOException;
}
