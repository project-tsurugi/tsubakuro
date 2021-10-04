package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * FutureQueryResponseImpl type.
 */
public class FutureQueryResponseImpl implements Future<ResponseProtos.ExecuteQuery> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionWireImpl sessionWireImpl;
    private ResponseWireHandleImpl responseWireHandleImpl;

    /**
     * Class constructor, called from SessionWireImpl that is connected to the SQL server.
     * @param sessionWireImpl the wireImpl class responsible for this communication
     * @param responseWireHandleImpl the handle indicating the responseWire by which a response message is to be transferred
     */
    FutureQueryResponseImpl(SessionWireImpl sessionWireImpl, ResponseWireHandleImpl responseWireHandleImpl) {
	this.sessionWireImpl = sessionWireImpl;
	this.responseWireHandleImpl = responseWireHandleImpl;
    }
	
    /**
     * get the message received from the SQL server.
     */
    public ResponseProtos.ExecuteQuery get() throws ExecutionException {
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

    public ResponseProtos.ExecuteQuery get(long timeout, TimeUnit unit) throws ExecutionException {
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