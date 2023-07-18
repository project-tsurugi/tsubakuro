package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

public abstract class Link implements ServerResource {
    protected ResponseBox responseBox = new ResponseBox(this);
    protected TimeUnit timeUnit;
    protected long timeout = 0;

    /**
     * Send request message via this link to the server.
     * @param s the slot number for the responseBox
     * @param frameHeader the frameHeader of the request
     * @param payload the payload of the request
     */
    public abstract void send(int s, byte[] frameHeader, byte[] payload, ChannelResponse channelResponse);

    /**
     * Create a ResultSetWire without a name, meaning that this link is not connected
     * @return ResultSetWire
    */
    public abstract ResultSetWire createResultSetWire() throws IOException;

    /**
     * Sets close timeout.
     * @param t the timeout
     */
    public void setCloseTimeout(Timeout t) {
        timeout = t.value();
        timeUnit = t.unit();
    }

    /**
     * Pull a response message from this link.
     * @param t the timeout value
     * @param u the timeout unit
     * @return true if the pull is successful, otherwise false
     * @throws TimeoutException if Timeout error was occurred while pulling response message,
     *      which won't be occured when t is 0
     */
    public abstract boolean pull(long t, TimeUnit u) throws TimeoutException;

    /**
     * Provide dead/alive information of this link
     * @return true when the server is alive
     */
    public abstract boolean isAlive();

    ResponseBox getResponseBox() {
        return responseBox;
    }

    // to suppress spotbug error
    long value() {
        return this.timeout;
    }
    TimeUnit unit() {
        return this.timeUnit;
    }
}
