package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.channel.common.connection.ClientInformation;
import com.tsurugidb.tsubakuro.channel.common.connection.Connector;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * StreamConnectorImpl type.
 */
public final class StreamConnectorImpl implements Connector {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConnectorImpl.class);

    public static final int DEFAULT_PORT = 12345;
    private final String hostname;
    private final int port;

    public StreamConnectorImpl(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public FutureResponse<Wire> connect(@Nonnull ClientInformation clientInformation) throws IOException {
        LOG.trace("will connect to {}:{}", hostname, port); //$NON-NLS-1$
        return new FutureStreamWireImpl(new StreamLink(hostname, port), clientInformation);
    }
}
