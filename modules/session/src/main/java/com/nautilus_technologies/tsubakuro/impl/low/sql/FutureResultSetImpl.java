package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * FutureResultSetImpl type.
 */
public class FutureResultSetImpl implements Future<ResultSet> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private Future<ResponseProtos.ExecuteQuery> future;
    private ResultSetImpl resultSetImpl;

    /**
     * Class constructor, called from TransactionImpl that executed the SQL that created this result set.
     * @param future the Future<ResponseProtos.ExecuteQuery>
     * @param sessionLinkImpl the sessionLink to which the transaction that created this object belongs
     */
    FutureResultSetImpl(Future<ResponseProtos.ExecuteQuery> future, SessionLinkImpl sessionLinkImpl, Future<ResponseProtos.ResultOnly> futureResponse) throws IOException {
	this.future = future;
	this.resultSetImpl = new ResultSetImpl(sessionLinkImpl.createResultSetWire(), futureResponse);
    }

    /**
     * Class constructor used when an error occured in SQL server.
     * @param future the Future<ResponseProtos.ResultOnly>
     * @param sessionLinkImpl the sessionLink to which the transaction that created this object belongs
     */
    FutureResultSetImpl(Future<ResponseProtos.ResultOnly> futureResponse) throws IOException {
	this.resultSetImpl = new ResultSetImpl(futureResponse);
    }

    public ResultSetImpl get() throws ExecutionException {
	try {
	    ResponseProtos.ExecuteQuery response = future.get();
	    if (Objects.isNull(response)) {
		return null;
	    }
	    resultSetImpl.connect(response.getName(), response.getRecordMeta());
	    return resultSetImpl;
	} catch (IOException e) {
            throw new ExecutionException(e);
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public ResultSetImpl get(long timeout, TimeUnit unit) throws TimeoutException, ExecutionException {
	try {
	    ResponseProtos.ExecuteQuery response = future.get(timeout, unit);
	    if (Objects.isNull(response)) {
		return null;
	    }
	    resultSetImpl.connect(response.getName(), response.getRecordMeta());
	    return resultSetImpl;
	} catch (IOException | InterruptedException e) {
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
