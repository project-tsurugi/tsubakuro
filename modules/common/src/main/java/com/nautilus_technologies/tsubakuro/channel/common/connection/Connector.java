package com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;

/**
 * Connector type.
 */
public interface Connector {
    /**
     * Connect to the sql service
     * @return future session wire
     * @throws IOException connection error
     */
    Future<SessionWire> connect() throws IOException;
}
