package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.Session;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;

/**
 * FutureSessionImpl type.
 */
public class FutureSessionImpl implements Future<Session> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    Future<SessionWire> sessionWire;
    
    FutureSessionImpl(Future<SessionWire> wire) {
	this.sessionWire = wire;
    }

    public Session get() throws ExecutionException {
	try {
	    return new SessionImpl(sessionWire.get());
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public Session get(long timeout, TimeUnit unit) throws ExecutionException {
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
