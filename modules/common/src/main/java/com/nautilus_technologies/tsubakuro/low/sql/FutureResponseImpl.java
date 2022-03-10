package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;

/**
 * FutureResponseImpl type.
 */
public class FutureResponseImpl<V> implements Future<V> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionWire sessionWireImpl;
    private Distiller<V> distiller;
    private ResponseWireHandle responseWireHandleImpl;

    /**
     * Class constructor, called from SessionWire that is connected to the SQL server.
     * @param sessionWireImpl the wireImpl class responsible for this communication
     * @param distiller the Distiller class that will work for the message to be received
     * @param responseWireHandleImpl the handle indicating the responseWire by which a response message is to be transferred
     */
    FutureResponseImpl(SessionWire sessionWireImpl, Distiller<V> distiller) {
	this.sessionWireImpl = sessionWireImpl;
	this.distiller = distiller;
    }
    public void setResponseHandle(ResponseWireHandle handle) {
	responseWireHandleImpl = handle;
    }

    /**
     * get the message received from the SQL server.
     */
    public V get() throws ExecutionException {
	if (Objects.isNull(responseWireHandleImpl)) {
	    throw new ExecutionException(new IOException("request has not been send out"));
	}
	try {
	    return distiller.distill(sessionWireImpl.receive(responseWireHandleImpl));
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public V get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	if (Objects.isNull(responseWireHandleImpl)) {
	    throw new ExecutionException(new IOException("request has not been send out"));
	}
	try {
	    return distiller.distill(sessionWireImpl.receive(responseWireHandleImpl, timeout, unit));
	} catch (IOException e) {
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
