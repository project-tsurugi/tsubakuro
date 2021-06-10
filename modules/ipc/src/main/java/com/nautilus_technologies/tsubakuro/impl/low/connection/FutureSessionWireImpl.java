package com.nautilus_technologies.tsubakuro.impl.low.connection;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.impl.low.sql.SessionWireImpl;

/**
 * FutureSessionWireImpl type.
 */
public class FutureSessionWireImpl implements Future<SessionWire> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    String name;
    long handle;
    
    FutureSessionWireImpl(String name, long handle) {
	this.handle = handle;
	this.name = name;
    }

    public SessionWire get() throws ExecutionException {
	try {
	    return IpcConnectorImpl.getSessionWire(name, handle);
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public SessionWire get(long timeout, TimeUnit unit) throws ExecutionException {
	return get();  // FIXME need to be implemented properly, same as below
    }
    public boolean isDone() {
	return isDone || IpcConnectorImpl.checkConnection(name, handle);
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
