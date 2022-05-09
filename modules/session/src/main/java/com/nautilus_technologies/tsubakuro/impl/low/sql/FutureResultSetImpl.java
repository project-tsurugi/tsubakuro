package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * FutureResultSetImpl type.
 */
public class FutureResultSetImpl extends AbstractFutureResponse<ResultSet> {
    private FutureResponse<ResponseProtos.ExecuteQuery> future;
    private final ResultSetImpl resultSetImpl;

    /**
     * Class constructor, called from TransactionImpl that executed the SQL that created this result set.
     * @param future the FutureResponse<ResponseProtos.ExecuteQuery>
     * @param sessionLinkImpl the sessionLink to which the transaction that created this object belongs
     */
    FutureResultSetImpl(
            FutureResponse<ResponseProtos.ExecuteQuery> future,
            SessionLinkImpl sessionLinkImpl,
            FutureResponse<ResponseProtos.ResultOnly> futureResponse) throws IOException {
        this.future = future;
        this.resultSetImpl = new ResultSetImpl(sessionLinkImpl.createResultSetWire(), futureResponse);
    }

    /**
     * Class constructor used when an error occured in SQL server.
     * @param future the FutureResponse<ResponseProtos.ResultOnly>
     * @param sessionLinkImpl the sessionLink to which the transaction that created this object belongs
     */
    FutureResultSetImpl(FutureResponse<ResponseProtos.ResultOnly> futureResponse) throws IOException {
        this.resultSetImpl = new ResultSetImpl(futureResponse);
    }

    @Override
    protected ResultSet getInternal() throws IOException, ServerException, InterruptedException {
        ResponseProtos.ExecuteQuery response = future.get();
        if (Objects.nonNull(response)) {
            resultSetImpl.connect(response.getName(), response.getRecordMeta());
        }
        return resultSetImpl;
    }

    @Override
    protected ResultSet getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        ResponseProtos.ExecuteQuery response = future.get(timeout, unit);
        if (Objects.nonNull(response)) {
            resultSetImpl.connect(response.getName(), response.getRecordMeta());
        }
        return resultSetImpl;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        future.close();
    }
}
