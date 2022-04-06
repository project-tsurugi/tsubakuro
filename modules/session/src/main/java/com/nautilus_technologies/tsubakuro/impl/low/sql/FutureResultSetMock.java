package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;

/**
 * FutureResultSetMock type.
 */
public class FutureResultSetMock implements Future<ResultSet> {
    private boolean isDone = false;
    private boolean isCancelled = false;
    private boolean isOk;

    public FutureResultSetMock(boolean isOk) {
	this.isOk = isOk;
    }

    public ResultSet get() throws ExecutionException {
	if (isOk) {
	    return new ResultSetMock();
	}
	return null;
    }

    public ResultSet get(long timeout, TimeUnit unit) throws ExecutionException {
	return get();
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
