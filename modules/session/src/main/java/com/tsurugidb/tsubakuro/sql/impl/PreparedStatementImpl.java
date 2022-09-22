package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;

/**
 * PreparedStatementImpl type.
 */
public class PreparedStatementImpl implements PreparedStatement {
    static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    private final SqlService service;
    private final ServerResource.CloseHandler closeHandler;
    private long timeout = 0;
    private TimeUnit unit;
    SqlCommon.PreparedStatement handle;

    public PreparedStatementImpl(SqlCommon.PreparedStatement handle, SqlService service, ServerResource.CloseHandler closeHandler) {
        this.handle = handle;
        this.service = service;
        this.closeHandler = closeHandler;
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
            try (var futureResponse = service.send(SqlRequest.DisposePreparedStatement.newBuilder().setPreparedStatementHandle(handle).build())) {
                if (timeout == 0) {
                    futureResponse.get();
                } else {
                    futureResponse.get(timeout, unit);
                }
            } catch (TimeoutException e) {
                LOG.warn("closing resource is timeout", e);
            }
        }
        if (Objects.nonNull(closeHandler)) {
            Lang.suppress(
                    e -> LOG.warn("error occurred while collecting garbage", e),
                    () -> closeHandler.onClosed(this));
        }
        handle = null;
    }
}