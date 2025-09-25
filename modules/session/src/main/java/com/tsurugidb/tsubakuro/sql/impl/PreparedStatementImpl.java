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
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.Disposer;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlService;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * PreparedStatementImpl type.
 */
public class PreparedStatementImpl implements PreparedStatement {
    static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    // A time short enough to cause a timeout if no message arrives during the acquisition operation,
    // measured in microseconds.
    static final long VERY_SHORT_TIMEOUT = 1000;
    // The time until the disposer gives up on closing
    static final long GIVE_UP_CLOSE_IN_SECONDS = 300;

    private final SqlService service;
    private final ServerResource.CloseHandler closeHandler;
    // timeout == 0 means no timeout is applied when closing (waits indefinitely)
    private long timeout = 0;
    private TimeUnit unit = null;
    private FutureResponse<Void> futureResponse = null;
    final SqlCommon.PreparedStatement handle;
    private final SqlRequest.Prepare request;
    private Disposer disposer = null;
    private boolean addedToDisposer = false;
    private boolean closed = false;
    private Instant closeInvokedInstant = null;

    /**
     * Creates a new instance.
     * @param handle the SqlCommon.PreparedStatement
     * @param service the SQL service
     * @param closeHandler handles {@link #close()} was invoked
     * @param request the request origin of the PreparedStatement
     * @param disposer the Disposer in charge of its asynchronous close
     */
    public PreparedStatementImpl(
            @Nonnull SqlCommon.PreparedStatement handle,
            @Nullable SqlService service,
            @Nullable ServerResource.CloseHandler closeHandler,
            @Nullable SqlRequest.Prepare request,
            @Nullable Disposer disposer) {
        Objects.requireNonNull(handle);
        this.handle = handle;
        this.service = service;
        this.closeHandler = closeHandler;
        this.request = request;
        this.disposer = disposer;
    }

    /**
     * Creates a new instance for test purpose without service, closeHandle, request, and disposer.
     * @param handle the handle of the PreparedStatement
     * @deprecated as tests now always use disposer.
     */
    @Deprecated
    public PreparedStatementImpl(SqlCommon.PreparedStatement handle) {
        this(handle, null, null, null, null);
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
    public synchronized void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
    }

    @Override
    public synchronized void close() throws IOException, ServerException, InterruptedException {
        if (closeInvokedInstant == null) {
            closeInvokedInstant = Instant.now();
        }
        if (disposer != null) {
            if (!addedToDisposer) {
                disposer.add(new Disposer.DelayedClose() {
                    @Override
                    public boolean delayedClose() throws ServerException, IOException, InterruptedException {
                        return doClose();
                    }
                });
                addedToDisposer = true;
            }
            return;
        }
        doClose();
    }

    private boolean doClose() throws IOException, ServerException, InterruptedException {
        if (service != null) {
            if (futureResponse == null) {
                futureResponse = service.send(SqlRequest.DisposePreparedStatement.newBuilder().setPreparedStatementHandle(handle).build());
            }
            try {
                futureResponse.get(VERY_SHORT_TIMEOUT, TimeUnit.MICROSECONDS);
            } catch (ResponseTimeoutException | TimeoutException e) {
                var tillInstant = (timeout > 0 && unit != null) ? closeInvokedInstant.plusSeconds(unit.toSeconds(timeout)) : closeInvokedInstant.plusSeconds(GIVE_UP_CLOSE_IN_SECONDS);
                if (Instant.now().isBefore(tillInstant)) {
                    return false;
                }
                LOG.warn("closing resource is timeout", e);
                throw new IOException(e);
            }
        }
        if (closeHandler != null) {
            Lang.suppress(
                    e -> LOG.warn("error occurred while collecting garbage", e),
                    () -> closeHandler.onClosed(this));
        }
        closed = true;
        return true;
    }

    /**
     * Returns the prepared statement handle
     * @return the SqlCommon.PreparedStatement
     * @throws IOException if I/O error was occurred while obtaining the handle
     */
    public synchronized SqlCommon.PreparedStatement getHandle() throws IOException {
        if (closed) {
            throw new IOException("already closed");
        }
        return handle;
    }

    // for diagnostic
    synchronized String diagnosticInfo() {
        if (!closed) {
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
