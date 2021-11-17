package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * FutureResultSetImpl type.
 */
public class FutureResultSetImpl implements Future<ResultSet> {
    private boolean isDone = false;
    private boolean isCancelled = false;

    private SessionLinkImpl sessionLinkImpl;
    private Future<ResponseProtos.ExecuteQuery> future;

    /**
     * Class constructor, called from TransactionImpl that executed the SQL that created this result set.
     * @param future the Future<ResponseProtos.ExecuteQuery>
     * @param sessionLinkImpl the sessionLink to which the transaction that created this object belongs
     */
    FutureResultSetImpl(Future<ResponseProtos.ExecuteQuery> future, SessionLinkImpl sessionLinkImpl) {
	this.future = future;
	this.sessionLinkImpl = sessionLinkImpl;
    }

    public ResultSetImpl get() throws ExecutionException {
	try {
	    ResponseProtos.ExecuteQuery response = future.get();
	    if (Objects.isNull(response)) {
		return null;
	    }
	    return new ResultSetImpl(sessionLinkImpl.createResultSetWire(response.getName()), response.getRecordMeta());
	} catch (IOException e) {
            throw new ExecutionException(e);
	} catch (InterruptedException e) {
            throw new ExecutionException(e);
        }
    }

    public ResultSetImpl get(long timeout, TimeUnit unit) throws ExecutionException {
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
