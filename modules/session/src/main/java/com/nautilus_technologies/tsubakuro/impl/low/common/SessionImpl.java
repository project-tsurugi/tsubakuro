package com.nautilus_technologies.tsubakuro.impl.low.common;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.low.common.Session;
import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * SessionImpl type.
 */
public class SessionImpl extends Session {
    static final Logger LOG = LoggerFactory.getLogger(SessionImpl.class);

    private long timeout;
    private TimeUnit unit;
    private SessionLinkImpl sessionLinkImpl;
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
        sessionLinkImpl = new SessionLinkImpl(wire);
        sessionWire = wire;
    }

    public FutureResponse<? extends Response> send(long id, byte[] request) throws IOException {
        return sessionWire.send(id, request);
    }

    /**
     * Provide sessionLink for SqlClientImpl
     * @return sessionLinkImpl
     */
    public SessionLinkImpl getSessionLinkImpl() {
        return sessionLinkImpl;
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
        if (Objects.nonNull(sessionLinkImpl)) {
            sessionLinkImpl.setCloseTimeout(t, u);
        }
    }

    /**
     * Close the Session
     */
    @Override
    public void close() throws IOException, InterruptedException {
        if (Objects.nonNull(sessionLinkImpl)) {
            try {
                sessionLinkImpl.close();
            } catch (ServerException e) {
                LOG.warn("closing session is timeout", e);
            } finally {
                sessionLinkImpl = null;
            }
        }
        super.close();
    }
}
