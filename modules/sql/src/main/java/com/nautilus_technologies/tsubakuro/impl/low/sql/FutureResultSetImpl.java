package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * FutureResultSetImpl type.
 */
public class FutureResultSetImpl implements Future<ResultSet> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionWire sessionWire;
    private ResultSetWire resultSetWire;
    private FutureResponse<ResponseProtos.ExecuteQuery> future;
    
    FutureResultSetImpl(FutureResponse<ResponseProtos.ExecuteQuery> f, SessionWire w) {
	future = f;
	sessionWire = w;
    }

    public ResultSetImpl get() throws ExecutionException
    {
	try {
	    ResponseProtos.ExecuteQuery r = future.get();
	    return new ResultSetImpl(sessionWire.createResultSetWire(r.getName()));
	} catch (IOException e) {
            throw new ExecutionException(e);
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public ResultSetImpl get(long timeout, TimeUnit unit) throws ExecutionException { return get(); }
    public boolean isDone() { return isDone; }  // FIXME need to be implemented properly, same as below
    public boolean isCancelled() { return isCancelled; }
    public boolean cancel(boolean mayInterruptIfRunning) { isCancelled = true; isDone = true; return true; }
}
