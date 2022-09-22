package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureWireImpl type.
 */
public class FutureWireImpl implements FutureResponse<Wire> {

    StreamLink streamLink;

    FutureWireImpl(StreamLink streamLink) {
        this.streamLink = streamLink;
    }

    @Override
    public Wire get() throws IOException {
        var message = streamLink.helloResponse();  // mutual exclusion is unnecessay here
        var rc = message.getInfo();
        var rv = message.getString();
        if (rc == StreamLink.RESPONSE_SESSION_HELLO_OK) {
            return new WireImpl(streamLink, Long.parseLong(rv));
        }
        return null;
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws IOException {
        // FIXME: consider SO_TIMEOUT
        var message = streamLink.helloResponse();
        var rc = message.getInfo();
        var rv = message.getString();
        if (rc == StreamLink.RESPONSE_SESSION_HELLO_OK) {
            return new WireImpl(streamLink, Long.parseLong(rv));
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
