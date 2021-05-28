package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * FutureResponseImpl type.
 */
public class FutureResponseImpl<V> extends FutureResponse<V> {
    private boolean isCancelled = false;
    private boolean isDone = false;

    private WireImpl wire;
    private Distiller<V> distiller;
    private long responseHandle;

    FutureResponseImpl(WireImpl w, Distiller<V> d, long h) {
	wire = w;
	distiller = d;
	responseHandle = h;
    }
	
    public boolean cancel(boolean mayInterruptIfRunning) { isCancelled = true; return true; }
    public V get() throws ExecutionException
    {
	try {
	    return distiller.distill(wire.recv());
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }
    public V get(long timeout, TimeUnit unit) throws ExecutionException { return get(); }
    public boolean isCancelled() { return isCancelled; }
    public boolean isDone() { return isDone; }
}
