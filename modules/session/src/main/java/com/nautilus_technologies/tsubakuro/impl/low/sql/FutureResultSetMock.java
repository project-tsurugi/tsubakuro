package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.util.Lang;

/**
 * FutureResultSetMock type.
 */
// FIXME: remove mock code
public class FutureResultSetMock extends AbstractFutureResponse<ResultSet> {
    private final boolean isOk;

    public FutureResultSetMock(boolean isOk) {
        this.isOk = isOk;
    }

    @Override
    protected ResultSet getInternal() throws IOException, ServerException, InterruptedException {
        return new ResultSetMock(isOk);
    }

    @Override
    protected ResultSet getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        return get();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        Lang.pass();
    }
}
