package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.SqlService;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.tsurugidb.jogasaki.proto.SqlCommon;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;

/**
 * PreparedStatementImpl type.
 */
public class PreparedStatementImpl implements PreparedStatement {
    static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    private long timeout = 0;
    private TimeUnit unit;
    SqlCommon.PreparedStatement handle;
    private final SqlService service;

    public PreparedStatementImpl(SqlCommon.PreparedStatement handle, SqlService service) {
        this.handle = handle;
        this.service = service;
    }

    public SqlCommon.PreparedStatement getHandle() throws IOException {
        if (Objects.isNull(handle)) {
            throw new IOException("already closed");
        }
        return handle;
    }

    @Override
    public boolean hasResultRecords() {
        return handle.getHasResultRecords();
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
    @Override
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (Objects.nonNull(handle) && Objects.nonNull(service)) {
            try {
                var futureResponse = service.send(SqlRequest.DisposePreparedStatement.newBuilder().setPreparedStatementHandle(handle).build());
                var response = (timeout == 0) ? futureResponse.get() : futureResponse.get(timeout, unit);
                if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
                    throw new IOException(response.getError().getDetail());
                }
            } catch (TimeoutException e) {
                LOG.warn("closing resource is timeout", e);
            }
        }
        handle = null;
    }
}
