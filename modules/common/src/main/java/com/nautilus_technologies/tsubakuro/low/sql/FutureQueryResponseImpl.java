package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.low.sql.SessionWire;

/**
 * FutureQueryResponseImpl type.
 */
public class FutureQueryResponseImpl implements Future<ResponseProtos.ExecuteQuery> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionWire sessionWireImpl;
    private ResponseWireHandle responseWireHandleImpl;

    /**
     * Class constructor, called from SessionWire that is connected to the SQL server.
     * @param sessionWireImpl the wireImpl class responsible for this communication
     * @param responseWireHandleImpl the handle indicating the responseWire by which a response message is to be transferred
     */
    FutureQueryResponseImpl(SessionWire sessionWireImpl) {
	this.sessionWireImpl = sessionWireImpl;
    }
    public void setResponseHandle(ResponseWireHandle handle) {
	responseWireHandleImpl = handle;
    }

    /**
     * get the message received from the SQL server.
     */
    public ResponseProtos.ExecuteQuery get() throws ExecutionException {
	if (Objects.isNull(responseWireHandleImpl)) {
	    throw new ExecutionException(new IOException("request has not been send out"));
	}
	try {
	    var response = sessionWireImpl.receive(responseWireHandleImpl);
	    if (ResponseProtos.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
		return response.getExecuteQuery();
	    }
	    sessionWireImpl.unReceive(responseWireHandleImpl);
	    return null;
	} catch (IOException e) {
	    throw new ExecutionException(e);
	}
    }

    public ResponseProtos.ExecuteQuery get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	if (Objects.isNull(responseWireHandleImpl)) {
	    throw new ExecutionException(new IOException("request has not been send out"));
	}
	try {
	    var response = sessionWireImpl.receive(responseWireHandleImpl, timeout, unit);
	    if (ResponseProtos.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
		return response.getExecuteQuery();
	    }
	    sessionWireImpl.unReceive(responseWireHandleImpl);
	    return null;
	} catch (IOException e) {
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
