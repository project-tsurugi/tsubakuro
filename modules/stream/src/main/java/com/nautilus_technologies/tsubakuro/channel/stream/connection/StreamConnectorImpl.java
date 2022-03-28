package com.nautilus_technologies.tsubakuro.channel.stream.connection;

import java.util.concurrent.Future;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.common.connection.Connector;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;

/**
 * StreamConnectorImpl type.
 */
public final class StreamConnectorImpl implements Connector {
    public static final int DEFAULT_PORT = 12345; 
    private String hostname;
    private int port;
    
    public StreamConnectorImpl(String hostname, int port) {
	this.hostname = hostname;
	this.port = port;
    }
    
    public Future<SessionWire> connect() throws IOException {
	var streamWire = new StreamWire(hostname, port);
	streamWire.hello();
	return new FutureSessionWireImpl(streamWire);
    }
}
