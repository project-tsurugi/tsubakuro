/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.tsubakuro.util.ServerResource;

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
    private final SqlRequest.Prepare request;

    /**
     * Creates a new instance.
     * @param handle the SqlCommon.PreparedStatement
     * @param service the SQL service
     * @param closeHandler handles {@link #close()} was invoked
     * @param request the request origin of the PreparedStatement
     */
    public PreparedStatementImpl(SqlCommon.PreparedStatement handle, SqlService service, ServerResource.CloseHandler closeHandler, SqlRequest.Prepare request) {
        this.handle = handle;
        this.service = service;
        this.closeHandler = closeHandler;
        this.request = request;
    }

    /**
     * Creates a new instance without service, closeHandle, request for test purpose.
     * @param handle the handle of the PreparedStatement
     */
    public PreparedStatementImpl(SqlCommon.PreparedStatement handle) {
        this(handle, null, null, null);
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
        if (!closed.getAndSet(true) && service != null) {
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
        if (closeHandler != null) {
            Lang.suppress(
                    e -> LOG.warn("error occurred while collecting garbage", e),
                    () -> closeHandler.onClosed(this));
        }
    }

    /**
     * Returns the prepared statement handle
     * @return the SqlCommon.PreparedStatement
     * @throws IOException if I/O error was occurred while obtaining the handle
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
            String rv = " +PreparedStatement " + Long.valueOf(handle.getHandle()).toString() + System.getProperty("line.separator");
            if (request != null) {
                rv += "   ==== request from here ====" + System.getProperty("line.separator");
                rv += request.toString();
                rv += "   ==== request to here ====" + System.getProperty("line.separator");
            }
            return rv;
        }
        return "";
    }
}
