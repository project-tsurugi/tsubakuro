package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Response;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.low.sql.RelationCursor;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.ResultSetMetadata;
import com.nautilus_technologies.tsubakuro.low.sql.io.DateTimeInterval;
import com.nautilus_technologies.tsubakuro.util.Lang;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tsubakuro.util.Timeout;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

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

    private final FutureResponse<Void> futureResponse;

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
    }

    /**
     * Creates a new instance.
     * @param metadata the metadata
     * @param cursor the relation cursor to delegate
     * @param response the original response
     * @param checker tests if response is normal
     */
    public ResultSetImpl(
            @Nullable ServerResource.CloseHandler closeHandler,
            @Nonnull ResultSetMetadata metadata,
            @Nonnull RelationCursor cursor,
            @Nonnull Response response,
            @Nonnull ResponseTester checker,
            @Nonnull FutureResponse<Void> futureResponse) {
        Objects.requireNonNull(metadata);
        Objects.requireNonNull(cursor);
        Objects.requireNonNull(response);
        Objects.requireNonNull(checker);
        Objects.requireNonNull(futureResponse);
        this.closeHandler = closeHandler;
        this.metadata = metadata;
        this.cursor = cursor;
        this.response = response;
        this.tester = checker;
        this.futureResponse = futureResponse;
    }

    @Override
    public ResultSetMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean nextRow() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            checkResponse();
            try {
                return cursor.nextRow();
            } catch (IOException | ServerException e) {
                checkResponse(e);
                throw e;
            }
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public boolean nextColumn() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.nextColumn();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public boolean isNull() {
        return cursor.isNull();
    }

    @Override
    public boolean fetchBooleanValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchBooleanValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public int fetchInt4Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchInt4Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public long fetchInt8Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchInt8Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public float fetchFloat4Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchFloat4Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public double fetchFloat8Value() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchFloat8Value();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public BigDecimal fetchDecimalValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchDecimalValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public String fetchCharacterValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchCharacterValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public byte[] fetchOctetValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchOctetValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public boolean[] fetchBitValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchBitValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public LocalDate fetchDateValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchDateValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public LocalTime fetchTimeOfDayValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchTimeOfDayValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public Instant fetchTimePointValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchTimePointValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public DateTimeInterval fetchDateTimeIntervalValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.fetchDateTimeIntervalValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public int beginArrayValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.beginArrayValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public void endArrayValue() throws IOException, ServerException, InterruptedException {
        cursor.endArrayValue();
    }

    @Override
    public int beginRowValue() throws IOException, ServerException, InterruptedException {
        checkResponse();
        try {
            return cursor.beginRowValue();
        } catch (IOException | ServerException e) {
            checkResponse(e);
            throw e;
        }
    }

    @Override
    public void endRowValue() throws IOException, ServerException, InterruptedException {
        cursor.endRowValue();
    }

    private void checkResponse() throws IOException, ServerException, InterruptedException {
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
    public void setCloseTimeout(Timeout timeout) {
        cursor.setCloseTimeout(timeout);
    }

    @Override
    public FutureResponse<Void> getResponse() {
        return futureResponse;   
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        if (Objects.nonNull(response)) {
            try (response) {
                try {
                    cursor.close();
                } catch (Exception e) {
                    // suppresses exception while closing the sub-response
                    LOG.warn("error occurred while closing result set", e);
                }
    
                // check main response whether to finish the request normally
                if (tested.compareAndSet(false, true)) {
                    tester.test(response);
                }
            }
        }
        if (Objects.nonNull(closeHandler)) {
            Lang.suppress(
                    e -> LOG.warn("error occurred while collecting garbage", e),
                    () -> closeHandler.onClosed(this));
        }
    }
}
