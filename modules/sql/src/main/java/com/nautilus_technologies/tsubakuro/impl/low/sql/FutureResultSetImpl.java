package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * FutureResultSetImpl type.
 */
public class FutureResultSetImpl implements Future<ResultSet> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionLinkImpl sessionLink;
    private Future<ResponseProtos.ExecuteQuery> future;
    
    FutureResultSetImpl(Future<ResponseProtos.ExecuteQuery> future, SessionLinkImpl sessionLink) {
	this.future = future;
	this.sessionLink = sessionLink;
    }

    public ResultSetImpl get() throws ExecutionException {
	try {
	    ResponseProtos.ExecuteQuery response = future.get();
	    return new ResultSetImpl(sessionLink.createResultSetWire(response.getName()));
	} catch (IOException e) {
            throw new ExecutionException(e);
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public ResultSetImpl get(long timeout, TimeUnit unit) throws ExecutionException {
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
