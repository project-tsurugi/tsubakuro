package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * FutureExplainImpl type.
 */
public class FutureExplainImpl implements Future<String> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private Future<ResponseProtos.Explain> future;
    
    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     * @param future the Future of ResponseProtos.Explain
     */
    public FutureExplainImpl(Future<ResponseProtos.Explain> future) {
	this.future = future;
    }

    public String get() throws ExecutionException {
	try {
	    ResponseProtos.Explain response = future.get();
	    if (ResponseProtos.Explain.ResultCase.ERROR.equals(response.getResultCase())) {
		throw new ExecutionException(new IOException(response.getError().getDetail()));
	    }
	    return response.getOutput();
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public String get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	try {
	    ResponseProtos.Explain response = future.get(timeout, unit);
	    if (ResponseProtos.Explain.ResultCase.ERROR.equals(response.getResultCase())) {
		throw new ExecutionException(new IOException(response.getError().getDetail()));
	    }
	    return response.getOutput();
	} catch (InterruptedException e) {
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
