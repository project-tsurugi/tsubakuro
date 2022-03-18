package com.nautilus_technologies.tsubakuro.channel.ipc.connection;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;

/**
 * FutureSessionWireImpl type.
 */
public class FutureSessionWireImpl implements Future<SessionWire> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    IpcConnectorImpl connector;
    
    FutureSessionWireImpl(IpcConnectorImpl connector) {
	this.connector = connector;
    }

    public SessionWire get() throws ExecutionException {
	try {
	    return connector.getSessionWire();
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public SessionWire get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	try {
	    return connector.getSessionWire(timeout, unit);
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }
    public boolean isDone() {
	return isDone || connector.checkConnection();
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
