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
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.RelationCursor;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.ResultSetMetadata;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * A basic implementation of {@link ResultSet} which just delegate operations to
 * {@link RelationCursor}.
 */
public class ResultSetImpl implements ResultSet {

    static final Logger LOG = LoggerFactory.getLogger(ResultSetImpl.class);

    private final CloseHandler closeHandler;

    private final ResultSetMetadata metadata;

    private final RelationCursor cursor;

    private final Response response;

    private final ResponseTester tester;

    private final AtomicBoolean tested = new AtomicBoolean();

    private final String resultSetName;

    private long timeout = 0;

    private TimeUnit unit;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final Message request;

    /**
     * Tests if the response is valid.
     */
    public interface ResponseTester {

        /**
         * Tests if the response is valid.
         * @param response the response
         * @throws IOException if I/O error was occurred while receiving response
         * @throws ServerException if server error was occurred while processing the request
         * @throws InterruptedException if interrupted while receiving response
         */
        void test(@Nonnull Response response) throws IOException, ServerException, InterruptedException;

        /**
         * Tests if the response is valid.
         * @param response the response
         * @param timeout the maximum time to wait
         * @param unit the time unit of {@code timeout}
         * @throws IOException if I/O error was occurred while receiving response
         * @throws ServerException if server error was occurred while processing the request
         * @throws InterruptedException if interrupted while receiving response
         * @throws TimeoutException if the wait time out
         */
        void test(@Nonnull Response response, long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException;
    }

    /**
     * Creates a new instance.
     * @param closeHandler handles {@link #close()} was invoked
     * @param metadata the metadata
     * @param cursor the relation cursor to delegate
     * @param response the original response
     * @param checker tests if response is normal
     * @param resultSetName the name of the ResultSetImpl
     * @param request the request origin of the ResultSetImpl
     */
    public ResultSetImpl(
            @Nullable ServerResource.CloseHandler closeHandler,
            @Nonnull ResultSetMetadata metadata,
            @Nonnull RelationCursor cursor,
            @Nonnull Response response,
            @Nonnull ResponseTester checker,
            @Nonnull String resultSetName,
            @Nonnull Message request) {
        Objects.requireNonNull(metadata);
        Objects.requireNonNull(cursor);
        Objects.requireNonNull(response);
        Objects.requireNonNull(checker);
        Objects.requireNonNull(resultSetName);
        Objects.requireNonNull(request);
        this.closeHandler = closeHandler;
        this.metadata = metadata;
        this.cursor = cursor;
        this.response = response;
        this.tester = checker;
        this.resultSetName = resultSetName;
        this.request = request;
    }

    @Override
    public ResultSetMetadata getMetadata() {
        return metadata;
    }

    @Override
    public synchronized boolean nextRow() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            if (cursor.nextRow()) {
                return true;
            }
            // check main response whether to finish the request normally
            if (tested.compareAndSet(false, true)) {
                try {
                    tester.test(response, timeout, unit);
                } catch (TimeoutException e) {
                    throw new ResponseTimeoutException(e);
                }
            }
            return false;
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized boolean nextColumn() throws IOException, ServerException, InterruptedException {
            checkResponse();
            try {
                return cursor.nextColumn();
            } catch (IOException | ServerException e) {
                checkResponse(e);
                throw e;
            }
    }

    @Override
    public synchronized boolean isNull() {
        return cursor.isNull();
    }

    @Override
    public synchronized boolean fetchBooleanValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchBooleanValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized int fetchInt4Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchInt4Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized long fetchInt8Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchInt8Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized float fetchFloat4Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchFloat4Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized double fetchFloat8Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchFloat8Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized BigDecimal fetchDecimalValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchDecimalValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized String fetchCharacterValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchCharacterValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized byte[] fetchOctetValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchOctetValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized boolean[] fetchBitValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchBitValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized LocalDate fetchDateValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchDateValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized LocalTime fetchTimeOfDayValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchTimeOfDayValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized LocalDateTime fetchTimePointValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchTimePointValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }


    @Override
    public synchronized OffsetTime fetchTimeOfDayWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchTimeOfDayWithTimeZoneValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized OffsetDateTime fetchTimePointWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchTimePointWithTimeZoneValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }


    @Override
    public synchronized DateTimeInterval fetchDateTimeIntervalValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchDateTimeIntervalValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized int beginArrayValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.beginArrayValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized void endArrayValue() throws IOException, ServerException, InterruptedException {
        cursor.endArrayValue();
    }

    @Override
    public synchronized int beginRowValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.beginRowValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public synchronized void endRowValue() throws IOException, ServerException, InterruptedException {
        cursor.endRowValue();
    }

    private void checkResponse() throws IOException, ServerException, InterruptedException {
        if (closed.get()) {
            throw new IOException("resultSet already closed");
        }
        if (response.isMainResponseReady()) {
            tested.set(true);
            tester.test(response);
        }
    }

    private void checkResponse(Exception e) throws IOException, ServerException, InterruptedException {
        try {
            // first, close upstream input
            cursor.close();
        } catch (Exception suppress) {
            // the exception in closing stream should be suppressed
            e.addSuppressed(suppress);
        }
        try {
            // then, check the main response
            tested.set(true);
            tester.test(response);
        } catch (Throwable inMain) {
            // throw exceptions in main response instead of
            inMain.addSuppressed(e);
            throw inMain;
        }
    }

    @Override
    public synchronized void setCloseTimeout(@Nullable Timeout t) {
        if (t != null) {
            timeout = t.value();
            unit = t.unit();
            cursor.setCloseTimeout(t);
        }
    }

    @Override
    public synchronized void close() throws ServerException, IOException, InterruptedException {
        if (!closed.getAndSet(true)) {
            try (response) {
                try {
                    cursor.close();
                } catch (Exception e) {
                    // suppresses exception while closing the sub-response
                    LOG.warn("error occurred while closing result set", e);
                }

                // check main response whether to finish the request normally
                if (tested.compareAndSet(false, true)) {
                    tester.test(response, timeout, unit);
                }
            } catch (TimeoutException e) {
                throw new ResponseTimeoutException(e);
            } finally {
                if (closeHandler != null) {
                    Lang.suppress(
                            e -> LOG.warn("error occurred while collecting garbage", e),
                            () -> closeHandler.onClosed(this));
                }
            }
        }
    }

    // for diagnostic
    String diagnosticInfo() {
        if (!closed.get()) {
            return " +ResulSet name = " + resultSetName + System.getProperty("line.separator")
                + "   ==== request from here ====" + System.getProperty("line.separator")
                + request.toString()
                + "   ==== request to here ====" + System.getProperty("line.separator");
        }
        return "";
    }
}
