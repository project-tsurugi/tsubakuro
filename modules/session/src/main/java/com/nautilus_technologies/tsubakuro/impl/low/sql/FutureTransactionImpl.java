package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.nautilus_technologies.tsubakuro.low.sql.Transaction;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * FutureTransactionImpl type.
 */
public class FutureTransactionImpl extends AbstractFutureResponse<Transaction> {
    private final FutureResponse<ResponseProtos.Begin> delegate;
    SessionLinkImpl sessionLinkImpl;

    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     * @param future the FutureResponse of ResponseProtos.Prepare
     * @param sessionLinkImpl the caller of this constructor
     */
    public FutureTransactionImpl(FutureResponse<ResponseProtos.Begin> future, SessionLinkImpl sessionLinkImpl) {
        this.delegate = future;
        this.sessionLinkImpl = sessionLinkImpl;
    }

    @Override
    protected Transaction getInternal() throws IOException, ServerException, InterruptedException {
        ResponseProtos.Begin response = delegate.get();
        return new TransactionImpl(response.getTransactionHandle(), sessionLinkImpl);
    }

    @Override
    protected Transaction getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        ResponseProtos.Begin response = delegate.get(timeout, unit);
        return new TransactionImpl(response.getTransactionHandle(), sessionLinkImpl);
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        delegate.close();
    }
}
