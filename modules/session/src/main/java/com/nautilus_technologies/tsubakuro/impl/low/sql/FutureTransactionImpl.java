package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * FutureTransactionImpl type.
 */
public class FutureTransactionImpl implements Future<Transaction> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private Future<ResponseProtos.Begin> future;
    SessionLinkImpl sessionLinkImpl;
    
    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     * @param future the Future of ResponseProtos.Prepare
     * @param sessionLinkImpl the caller of this constructor
     */
    public FutureTransactionImpl(Future<ResponseProtos.Begin> future, SessionLinkImpl sessionLinkImpl) {
	this.future = future;
	this.sessionLinkImpl = sessionLinkImpl;
    }

    public TransactionImpl get() throws ExecutionException {
	try {
	    ResponseProtos.Begin response = future.get();
	    return new TransactionImpl(response.getTransactionHandle(), sessionLinkImpl);
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public TransactionImpl get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	try {
	    ResponseProtos.Begin response = future.get(timeout, unit);
	    return new TransactionImpl(response.getTransactionHandle(), sessionLinkImpl);
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
