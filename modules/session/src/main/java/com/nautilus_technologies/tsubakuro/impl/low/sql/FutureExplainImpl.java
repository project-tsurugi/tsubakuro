package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * FutureExplainImpl type.
 */
public class FutureExplainImpl extends AbstractFutureResponse<String> {

    private final FutureResponse<ResponseProtos.Explain> delegate;

    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     * @param future the Future of ResponseProtos.Explain
     */
    public FutureExplainImpl(FutureResponse<ResponseProtos.Explain> future) {
        this.delegate = future;
    }

    @Override
    protected String getInternal() throws IOException, ServerException, InterruptedException {
        ResponseProtos.Explain response = delegate.get();
        return resolve(response);
    }

    @Override
    protected String getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        ResponseProtos.Explain response = delegate.get(timeout, unit);
        return resolve(response);
    }

    private String resolve(ResponseProtos.Explain response) throws IOException {
        if (ResponseProtos.Explain.ResultCase.ERROR.equals(response.getResultCase())) {
            // FIXME: throw structured exception
            throw new IOException(response.getError().getDetail());
        }
        return response.getOutput();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        delegate.close();
    }
}
