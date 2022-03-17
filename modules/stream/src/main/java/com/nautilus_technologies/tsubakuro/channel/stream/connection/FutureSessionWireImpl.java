package com.nautilus_technologies.tsubakuro.channel.stream.connection;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;
import com.nautilus_technologies.tsubakuro.channel.stream.sql.SessionWireImpl;

/**
 * FutureSessionWireImpl type.
 */
public class FutureSessionWireImpl implements Future<SessionWire> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    StreamConnectorImpl streamConnector;
    StreamWire streamWire;
    
    FutureSessionWireImpl(StreamConnectorImpl streamConnector, StreamWire streamWire) {
	this.streamConnector = streamConnector;
	this.streamWire = streamWire;
    }

    public SessionWire get() throws ExecutionException {
	try {
	    streamWire.receive();
	    var rc = streamWire.getInfo();
	    var rv = streamWire.getString();
	    streamWire.release();
	    if (rc == streamWire.STATUS_OK) {
		return new SessionWireImpl(streamWire, rv, streamConnector);
	    }
	    return null;
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public SessionWire get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	try {
	    streamWire.receive();
	    if (streamWire.getInfo() == streamWire.STATUS_OK) {
		return new SessionWireImpl(streamWire, streamWire.getString(), streamConnector);
	    }
	    return null;
	} catch (Exception e) {
	    throw new ExecutionException(e);
	}
    }
    public boolean isDone() {
	return isDone;
    }
    public boolean isCancelled() {
	return isCancelled;
    }
    public boolean cancel(boolean mayInterruptIfRunning) {
	isCancelled = true;
	isDone = true;
	return true;
    }
}
