package com.nautilus_technologies.tsubakuro.low.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import java.io.Closeable;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;

/**
 * Connector type.
 */
public interface Connector extends Closeable {
    /**
     * Connect to the sql service
     * @param name the database name to connect
     * @return the session
     */
    Future<SessionWire> connect() throws IOException;
}
