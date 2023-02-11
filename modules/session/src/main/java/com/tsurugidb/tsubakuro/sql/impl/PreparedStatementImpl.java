package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean closed = new AtomicBoolean();
    private long timeout = 0;
    private TimeUnit unit;
    final SqlCommon.PreparedStatement handle;

    /**
     * Creates a new instance.
     * @param handle the SqlCommon.PreparedStatement
     * @param service the SQL service
     * @param closeHandler handles {@link #close()} was invoked
     */
    public PreparedStatementImpl(SqlCommon.PreparedStatement handle, SqlService service, ServerResource.CloseHandler closeHandler) {
        this.handle = handle;
        this.service = service;
        this.closeHandler = closeHandler;
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
        if (!closed.getAndSet(true) && Objects.nonNull(service)) {
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
    }

    /**
     * Returns the prepared statement handle
     * @return the SqlCommon.PreparedStatement
     */
    public SqlCommon.PreparedStatement getHandle() throws IOException {
        if (closed.get()) {
            throw new IOException("already closed");
        }
        return handle;
    }

    // for diagnostic
    String diagnosticInfo() {
        if (!closed.get()) {
            return " +PreparedStatement " + Long.valueOf(handle.getHandle()).toString() + System.getProperty("line.separator");
        }
        return "";
    }
}
