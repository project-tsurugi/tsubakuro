package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * FutureExplainImpl type.
 */
public class FutureExplainImpl extends AbstractFutureResponse<String> {

    private final FutureResponse<SqlResponse.Explain> delegate;

    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     * @param future the Future of SqlResponse.Explain
     */
    public FutureExplainImpl(FutureResponse<SqlResponse.Explain> future) {
        this.delegate = future;
    }

    @Override
    protected String getInternal() throws IOException, ServerException, InterruptedException {
        SqlResponse.Explain response = delegate.get();
        return resolve(response);
    }

    @Override
    protected String getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        SqlResponse.Explain response = delegate.get(timeout, unit);
        return resolve(response);
    }

    private String resolve(SqlResponse.Explain response) throws IOException {
        if (SqlResponse.Explain.ResultCase.ERROR.equals(response.getResultCase())) {
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
