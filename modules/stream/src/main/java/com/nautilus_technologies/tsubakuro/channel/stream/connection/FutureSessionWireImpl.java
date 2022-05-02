package com.nautilus_technologies.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;
import com.nautilus_technologies.tsubakuro.channel.stream.sql.SessionWireImpl;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Lang;

/**
 * FutureSessionWireImpl type.
 */
public class FutureSessionWireImpl implements FutureResponse<SessionWire> {

    StreamWire streamWire;

    FutureSessionWireImpl(StreamWire streamWire) {
        this.streamWire = streamWire;
    }

    @Override
    public SessionWire get() throws IOException {
        streamWire.receive();
        var rc = streamWire.getInfo();
        var rv = streamWire.getString();
        streamWire.release();
        if (rc == StreamWire.RESPONSE_SESSION_HELLO_OK) {
            return new SessionWireImpl(streamWire, Long.parseLong(rv));
        }
        return null;
    }

    @Override
    public SessionWire get(long timeout, TimeUnit unit) throws IOException {
        // FIXME: consider SO_TIMEOUT
        streamWire.receive();
        var rc = streamWire.getInfo();
        var rv = streamWire.getString();
        streamWire.release();
        if (rc == StreamWire.RESPONSE_SESSION_HELLO_OK) {
            return new SessionWireImpl(streamWire, Long.parseLong(rv));
        }
        return null;
    }

    @Override
    public boolean isDone() {
        // FIXME: return status
        return false;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        Lang.pass();
    }
}
