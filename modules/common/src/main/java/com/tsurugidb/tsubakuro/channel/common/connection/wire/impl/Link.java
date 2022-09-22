package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ServerResource;

public abstract class Link implements ServerResource {

    protected ResponseBox responseBox = new ResponseBox(this);

    public abstract void send(int s, byte[] frameHeader, byte[] payload) throws IOException;

    public abstract ResultSetWire createResultSetWire() throws IOException;

    public ResponseBox getResponseBox() {
        return responseBox;
    }
}
