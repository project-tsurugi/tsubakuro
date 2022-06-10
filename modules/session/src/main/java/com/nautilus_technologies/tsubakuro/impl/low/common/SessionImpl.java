package com.nautilus_technologies.tsubakuro.impl.low.common;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * SessionImpl type.
 */
public class SessionImpl extends Session {
    static final Logger LOG = LoggerFactory.getLogger(SessionImpl.class);

    private long timeout;
    private TimeUnit unit;
    private SessionWire sessionWire;

    /**
     * Connect this session to the SQL server.
     *
     * Note. How to connect to a SQL server is implementation dependent.
     * This implementation assumes that the session wire connected to the database is given.
     *
     * @param sessionWire the wire that connects to the Database
     */
    public void connect(SessionWire wire) {
        super.wire = wire;
        sessionWire = wire;
    }

    public FutureResponse<? extends Response> send(long id, byte[] request) throws IOException {
        return sessionWire.send(id, request);
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
    }

    /**
     * Close the Session
     */
    @Override
    public void close() throws IOException, InterruptedException {
        super.close();
    }
}
