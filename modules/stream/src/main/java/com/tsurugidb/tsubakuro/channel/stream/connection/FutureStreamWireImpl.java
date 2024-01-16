package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.tsurugidb.endpoint.proto.EndpointRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureStreamWireImpl type.
 */
public class FutureStreamWireImpl implements FutureResponse<Wire> {

    private final ClientInformation clientInformation;
    StreamLink streamLink;
    private final AtomicBoolean gotton = new AtomicBoolean();

    FutureStreamWireImpl(StreamLink streamLink, @Nonnull ClientInformation clientInformation) {
        this.streamLink = streamLink;
        this.clientInformation = clientInformation;
    }

    private EndpointRequest.WireInformation wireInformation() {
        return EndpointRequest.WireInformation.newBuilder().setStreamInformation(
            EndpointRequest.WireInformation.StreamInformation.newBuilder().setMaximumConcurrentResultSets(ResponseBox.responseBoxSize())
        ).build();
    }

    @Override
    public Wire get() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            var wireImpl = new WireImpl(streamLink);
            var futureSessionID = wireImpl.handshake(clientInformation, wireInformation());
            wireImpl.setSessionID(futureSessionID.get());
            return wireImpl;
        }
        throw new IOException("FutureStreamWireImpl already closed.");
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
        if (!gotton.getAndSet(true)) {
            var wireImpl = new WireImpl(streamLink);
            var futureSessionID = wireImpl.handshake(clientInformation, wireInformation());
            wireImpl.setSessionID(futureSessionID.get(timeout, unit));
            return wireImpl;
        }
        throw new IOException("FutureStreamWireImpl already closed.");
    }

    @Override
    public boolean isDone() {
        // FIXME: return status
        return false;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            streamLink.close();
        }
    }
}
