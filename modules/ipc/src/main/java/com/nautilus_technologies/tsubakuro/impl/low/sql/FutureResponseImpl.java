package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * FutureResponseImpl type.
 */
public class FutureResponseImpl<V> implements Future<V> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionWireImpl wire;
    private Distiller<V> distiller;
    private ResponseHandleImpl handle;

    /**
     * Creates a new instance.
     * @param w the wireImpl class responsible for this communication
     * @param d the Distiller class that will work for the message to be received
     * @param h the handle indicating the message to be received in response to the outgoing message
     */
    FutureResponseImpl(SessionWireImpl w, Distiller<V> d, ResponseHandleImpl h) {
	wire = w;
	distiller = d;
	handle = h;
    }
	
    /**
     * get the message received from the SQL server.
     */
    public V get() throws ExecutionException {
	try {
	    return distiller.distill(wire.recv(handle));
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public V get(long timeout, TimeUnit unit) throws ExecutionException {
	return get();
    }
    public boolean isDone() {
	return isDone;  // FIXME need to be implemented properly, same as below
    }
    public boolean isCancelled() {
	return isCancelled;
    }
    public boolean cancel(boolean mayInterruptIfRunning) {
	isCancelled = true; isDone = true; return true;
    }
}
