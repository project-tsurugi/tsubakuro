package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.TableMetadata;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

/**
 * FutureTableMetadataImpl type.
 */
public class FutureTableMetadataImpl extends AbstractFutureResponse<TableMetadata> {

    private final FutureResponse<SqlResponse.DescribeTable> delegate;

    /**
     * Class constructor, called from SessionLinkImpl that is connected to the SQL server.
     * @param future the Future of SqlResponse.TableMetadata
     */
    public FutureTableMetadataImpl(FutureResponse<SqlResponse.DescribeTable> future) {
        this.delegate = future;
    }

    @Override
    protected TableMetadata getInternal() throws IOException, ServerException, InterruptedException {
        SqlResponse.DescribeTable response = delegate.get();
        return resolve(response);
    }

    @Override
    protected TableMetadata getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        SqlResponse.DescribeTable response = delegate.get(timeout, unit);
        return resolve(response);
    }

    private TableMetadata resolve(SqlResponse.DescribeTable response) throws IOException {
        if (SqlResponse.DescribeTable.ResultCase.ERROR.equals(response.getResultCase())) {
            // FIXME: throw structured exception
            throw new IOException(response.getError().getDetail());
        }
        return new TableMetadataAdapter(response.getSuccess());
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        delegate.close();
    }
}
