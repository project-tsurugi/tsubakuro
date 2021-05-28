package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * FutureResponse type.
 */
public class FutureResponse<V> implements Future<V> {
    private boolean isCancelled = false;
    private boolean isDone = false;

    private WireImpl wire;
    private Distiller<V> distiller;

    FutureResponse(WireImpl w, Distiller<V> d) {
	wire = w;
	distiller = d;
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

    public interface Distiller<V> {
	public abstract V distill(ResponseProtos.Response response);
    }
    public static class PrepareDistiller implements Distiller<ResponseProtos.Prepare> {
	public ResponseProtos.Prepare distill(ResponseProtos.Response response) {
	    return response.getPrepare();
	}
    }
    public static class ResultOnlyDistiller implements Distiller<ResponseProtos.ResultOnly> {
	public ResponseProtos.ResultOnly distill(ResponseProtos.Response response) {
	    return response.getResultOnly();
	}
    }
    public static class ExecuteQueryDistiller implements Distiller<ResponseProtos.ExecuteQuery> {
	public ResponseProtos.ExecuteQuery distill(ResponseProtos.Response response) {
	    return response.getExecuteQuery();
	}
    }
    public static class BeginDistiller implements Distiller<ResponseProtos.Begin> {
	public ResponseProtos.Begin distill(ResponseProtos.Response response) {
	    return response.getBegin();
	}
    }
}
