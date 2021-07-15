package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * FutureResponseImpl type.
 */
public class FutureResponseImpl<V> implements Future<V> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionWireImpl sessionWireImpl;
    private Distiller<V> distiller;
    private ResponseWireHandleImpl responseWireHandleImpl;

    /**
     * Class constructor, called from SessionWireImpl that is connected to the SQL server.
     * @param sessionWireImpl the wireImpl class responsible for this communication
     * @param distiller the Distiller class that will work for the message to be received
     * @param responseWireHandleImpl the handle indicating the responseWire by which a response message is to be transferred
     */
    FutureResponseImpl(SessionWireImpl sessionWireImpl, Distiller<V> distiller, ResponseWireHandleImpl responseWireHandleImpl) {
	this.sessionWireImpl = sessionWireImpl;
	this.distiller = distiller;
	this.responseWireHandleImpl = responseWireHandleImpl;
    }
	
    /**
     * get the message received from the SQL server.
     */
    public V get() throws ExecutionException {
	try {
	    return distiller.distill(sessionWireImpl.receive(responseWireHandleImpl));
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public V get(long timeout, TimeUnit unit) throws ExecutionException {
	return get();  // FIXME need to be implemented properly, same as below
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
