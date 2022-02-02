package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * FuturePreparedStatementImpl type.
 */
public class FuturePreparedStatementImpl implements Future<PreparedStatement> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private Future<ResponseProtos.Prepare> future;
    private SessionLinkImpl sessionLinkImpl;
    
    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     * @param future the Future<ResponseProtos.Prepare>
     * @param sessionLinkImpl the caller of this constructor
     */
    FuturePreparedStatementImpl(Future<ResponseProtos.Prepare> future, SessionLinkImpl sessionLinkImpl) {
	this.future = future;
	this.sessionLinkImpl = sessionLinkImpl;
    }

    public PreparedStatementImpl get() throws ExecutionException {
	try {
	    ResponseProtos.Prepare response = future.get();
	    if (ResponseProtos.Prepare.ResultCase.ERROR.equals(response.getResultCase())) {
		throw new ExecutionException(new IOException("prepare error"));
	    }
	    return new PreparedStatementImpl(response.getPreparedStatementHandle(), sessionLinkImpl);
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public PreparedStatementImpl get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	try {
	    ResponseProtos.Prepare response = future.get(timeout, unit);
	    if (ResponseProtos.Prepare.ResultCase.ERROR.equals(response.getResultCase())) {
		throw new ExecutionException(new IOException("prepare error"));
	    }
	    return new PreparedStatementImpl(response.getPreparedStatementHandle(), sessionLinkImpl);
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
