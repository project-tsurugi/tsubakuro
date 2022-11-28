package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;
public abstract class Link implements ServerResource {

    protected ResponseBox responseBox = new ResponseBox(this);
    protected TimeUnit timeUnit;
    protected long timeout = 0;

    public abstract void send(int s, byte[] frameHeader, byte[] payload, ChannelResponse channelResponse);

    public abstract ResultSetWire createResultSetWire() throws IOException;

    public ResponseBox getResponseBox() {
        return responseBox;
    }
    public void setCloseTimeout(Timeout t) {
        timeout = t.value();
        timeUnit = t.unit();
    }

    public abstract boolean isAlive();

    // to suppress spotbug error
    long value() {
        return this.timeout;
    }
    TimeUnit unit() {
        return this.timeUnit;
    }
}
