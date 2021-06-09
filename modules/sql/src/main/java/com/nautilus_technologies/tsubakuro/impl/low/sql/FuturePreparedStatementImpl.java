package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

/**
 * FuturePreparedStatementImpl type.
 */
public class FuturePreparedStatementImpl implements Future<PreparedStatement> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private Future<ResponseProtos.Prepare> future;
    
    FuturePreparedStatementImpl(Future<ResponseProtos.Prepare> future) {
	this.future = future;
    }

    public PreparedStatementImpl get() throws ExecutionException {
	try {
	    ResponseProtos.Prepare r = future.get();
	    return new PreparedStatementImpl(r.getPreparedStatementHandle());
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public PreparedStatementImpl get(long timeout, TimeUnit unit) throws ExecutionException {
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
