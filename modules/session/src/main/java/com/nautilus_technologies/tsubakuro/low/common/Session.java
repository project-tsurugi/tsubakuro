package com.nautilus_technologies.tsubakuro.low.common;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.FutureInputStream;
import com.nautilus_technologies.tsubakuro.util.ServerResource;

/**
 * Session type.
 */
public interface Session extends ServerResource {
    /**
     * Connect this session to the Database
     * @param sessionWire the wire that connects to the Database
     */
    void connect(SessionWire sessionWire);

    
    /**
     * Send a request via sessionWire
     * @param id identifies the service
     * @param request the request to the service
     * @return a FutureInputStream for response
     * @throws IOException error occurred in send
     */
    FutureInputStream send(long id, byte[] request) throws IOException;

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    void setCloseTimeout(long timeout, TimeUnit unit);
}
