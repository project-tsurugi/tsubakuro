package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * FutureTransactionImpl type.
 */
public class FutureTransactionImpl implements Future<Transaction> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private Future<ResponseProtos.Begin> future;
    SessionLinkImpl sessionLink;
    
    FutureTransactionImpl(SessionLinkImpl link, Future<ResponseProtos.Begin> future) {
	this.future = future;
	this.sessionLink = link;
    }

    public TransactionImpl get() throws ExecutionException {
	try {
	    ResponseProtos.Begin response = future.get();
	    return new TransactionImpl(sessionLink, response.getTransactionHandle());
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public TransactionImpl get(long timeout, TimeUnit unit) throws ExecutionException {
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
