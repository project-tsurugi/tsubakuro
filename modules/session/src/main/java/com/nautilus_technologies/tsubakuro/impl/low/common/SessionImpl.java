package com.nautilus_technologies.tsubakuro.impl.low.common;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.connection.Session;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * SessionImpl type.
 */
public class SessionImpl extends Session {
    static final Logger LOG = LoggerFactory.getLogger(SessionImpl.class);

    private long timeout;
    private TimeUnit unit;

    /**
     * Connect this session to the SQL server.
     *
     * Note. How to connect to a SQL server is implementation dependent.
     * This implementation assumes that the session wire connected to the database is given.
     *
     * @param Wire the wire that connects to the Database
     */
    public void connect(Wire wire) {
        super.wire = wire;
    }

    public FutureResponse<? extends Response> send(int id, byte[] request) throws IOException {
        return super.wire.send(id, request);
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
