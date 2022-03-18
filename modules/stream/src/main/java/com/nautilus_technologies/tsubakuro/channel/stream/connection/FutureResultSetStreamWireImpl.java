package com.nautilus_technologies.tsubakuro.channel.stream.connection;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;

/**
 * FutureResultSetStreamWireImpl type.
 */
public class FutureResultSetStreamWireImpl implements Future<StreamWire> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    StreamWire streamWire;
    
    FutureResultSetStreamWireImpl(StreamWire streamWire) {
	this.streamWire = streamWire;
    }

    public StreamWire get() throws ExecutionException {
	try {
	    streamWire.receive();
	    var rc = streamWire.getInfo();
	    streamWire.release();
	    if (rc == streamWire.STATUS_OK) {
		return streamWire;
	    }
	    return null;
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public StreamWire get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	try {
	    streamWire.receive();
	    if (streamWire.getInfo() == streamWire.STATUS_OK) {
		return streamWire;
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
