package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.stream.StreamWire;
import com.tsurugidb.tsubakuro.channel.stream.SessionWireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureSessionWireImpl type.
 */
public class FutureSessionWireImpl implements FutureResponse<Wire> {

    StreamWire streamWire;

    FutureSessionWireImpl(StreamWire streamWire) {
        this.streamWire = streamWire;
    }

    @Override
    public Wire get() throws IOException {
        var message = streamWire.receive();  // mutual exclusion is unnecessay here
        var rc = message.getInfo();
        var rv = message.getString();
        if (rc == StreamWire.RESPONSE_SESSION_HELLO_OK) {
            return new SessionWireImpl(streamWire, Long.parseLong(rv));
        }
        return null;
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws IOException {
        // FIXME: consider SO_TIMEOUT
        var message = streamWire.receive();  // mutual exclusion is unnecessay here
        var rc = message.getInfo();
        var rv = message.getString();
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
        // FIXME
    }
}
