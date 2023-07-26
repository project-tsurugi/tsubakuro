package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {
    private static final int SLEEP_UNIT = 10;  // 10mS per sleep
    public static final String METADATA_CHANNEL_ID = "metadata";
    public static final String RELATION_CHANNEL_ID = "relation";

    private final AtomicReference<ByteBuffer> main = new AtomicReference<>();
    private final AtomicReference<SqlResponse.ExecuteQuery> metadata = new AtomicReference<>();
    private final AtomicReference<ResultSetWire> resultSet = new AtomicReference<>();
    private final AtomicReference<IOException> exceptionMain = new AtomicReference<>();
    private final AtomicReference<IOException> exceptionResultSet = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean mainResponseGotton = new AtomicBoolean();
    private final Lock lock = new ReentrantLock();
    private final Condition noSet = lock.newCondition();
    private String resultSetName = "";  // for diagnostic

    /**
     * Creates a new instance
     */
    public ChannelResponse() {
    }

    @Override
    public boolean isMainResponseReady() {
        return Objects.nonNull(main.get()) || Objects.nonNull(exceptionMain.get());
    }

    @Override
    public ByteBuffer waitForMainResponse() throws IOException {
        if (Objects.nonNull(main.get())) {
            mainResponseGotton.set(true);
            return main.get();
        }
        if (Objects.nonNull(exceptionMain.get())) {
            var e = exceptionMain.get();
            throw new IOException(e.getMessage(), e);
        }

        while (true) {
            lock.lock();
            try {
                while (!isMainResponseReady()) {
                    noSet.await();
                }
                if (Objects.nonNull(main.get())) {
                    mainResponseGotton.set(true);
                    return main.get();
                }
                if (Objects.nonNull(exceptionMain.get())) {
                    var e = exceptionMain.get();
                    throw new IOException(e.getMessage(), e);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }
        if (Objects.nonNull(exceptionMain.get())) {
            var e = exceptionMain.get();
            throw new IOException(e.getMessage(), e);
        }

        while (true) {
            lock.lock();
            try {
                while (!isMainResponseReady()) {
                    if (!noSet.await(timeout, unit)) {
                        throw new TimeoutException("response has not arrived within the timeout specified");
                    }
                }
                if (Objects.nonNull(main.get())) {
                    mainResponseGotton.set(true);
                    return main.get();
                }
                if (Objects.nonNull(exceptionMain.get())) {
                    var e = exceptionMain.get();
                    throw new IOException(e.getMessage(), e);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public InputStream openSubResponse(String id) throws IOException, InterruptedException {
        if (id.equals(METADATA_CHANNEL_ID)) {
            waitForResultSetOrMainResponse();
            return metadataChannel();
        } else if (id.equals(RELATION_CHANNEL_ID)) {
            waitForResultSetOrMainResponse();
            return relationChannel();
        }
        throw new IOException("illegal SubResponse id");
    }

    @Override
    public InputStream openSubResponse(String id, long timeout, TimeUnit unit) throws IOException, InterruptedException, TimeoutException {
        if (id.equals(METADATA_CHANNEL_ID)) {
            waitForResultSetOrMainResponse(timeout, unit);
            return metadataChannel();
        } else if (id.equals(RELATION_CHANNEL_ID)) {
            waitForResultSetOrMainResponse(timeout, unit);
            return relationChannel();
        }
        throw new IOException("illegal SubResponse id");
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    private InputStream relationChannel() throws IOException, InterruptedException {
        if (Objects.nonNull(resultSet.get())) {
            return resultSet.get().getByteBufferBackedInput();
        }
        if (Objects.nonNull(exceptionResultSet.get())) {
            var e = exceptionResultSet.get();
            throw new IOException(e.getMessage(), e);
        }
        return null;
    }

    private InputStream metadataChannel() throws IOException, InterruptedException {
        if (Objects.nonNull(metadata.get())) {
            var recordMeta = metadata.get().getRecordMeta();
            return new ByteBufferInputStream(ByteBuffer.wrap(recordMeta.toByteArray()));
        }
        if (Objects.nonNull(exceptionResultSet.get())) {
            var e = exceptionResultSet.get();
            throw new IOException(e.getMessage(), e);
        }
        return null;
    }

    private void waitForResultSetOrMainResponse() throws IOException, InterruptedException {
        if (isResultSetReady() || (isMainResponseReady() && !mainResponseGotton.get())) {
            return;
        }

        lock.lock();
        try {
            while (!(isResultSetReady() || (isMainResponseReady() && !mainResponseGotton.get()))) {
                noSet.await();
            }
            return;
        } finally {
            lock.unlock();
        }
    }

    private void waitForResultSetOrMainResponse(long timeout, TimeUnit unit) throws IOException, InterruptedException, TimeoutException {
        if (isResultSetReady() || (isMainResponseReady() && !mainResponseGotton.get())) {
            return;
        }

        lock.lock();
        try {
            while (!(isResultSetReady() || (isMainResponseReady() && !mainResponseGotton.get()))) {
                if (!noSet.await(timeout, unit)) {
                    throw new TimeoutException("response has not arrived within the timeout specified");
                }
            }
            return;
        } finally {
            lock.unlock();
        }
    }

    private boolean isResultSetReady() {
        return Objects.nonNull(resultSet.get()) || Objects.nonNull(exceptionResultSet.get());
    }

    // get call from receiver thread
    void setMainResponse(@Nonnull ByteBuffer response) {
        Objects.requireNonNull(response);
        lock.lock();
        try {
            main.set(skipFrameworkHeader(response));
            noSet.signal();
        } catch (IOException e) {
            exceptionMain.set(e);
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }
    public void setMainResponse(@Nonnull IOException exception) {
        Objects.requireNonNull(exception);
        lock.lock();
        try {
            exceptionMain.set(exception);
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }

    public void setResultSet(@Nonnull ByteBuffer response, @Nonnull ResultSetWire resultSetWire) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(resultSetWire);
        lock.lock();
        try {
            var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(skipFrameworkHeader(response)));
            var detailResponse = sqlResponse.getExecuteQuery();
            resultSetName = detailResponse.getName();
            resultSetWire.connect(resultSetName);

            metadata.set(detailResponse);
            resultSet.set(resultSetWire);
            noSet.signal();
        } catch (IOException e) {
            exceptionResultSet.set(e);
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }

    private ByteBuffer skipFrameworkHeader(ByteBuffer response) throws IOException {
        response.rewind();
        var header = FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(response));
        if (header.getPayloadType() == com.tsurugidb.framework.proto.FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS) {
            var errorResponse = com.tsurugidb.diagnostics.proto.Diagnostics.Record.parseDelimitedFrom(new ByteBufferInputStream(response));
            throw new IOException(new CoreServiceException(CoreServiceCode.valueOf(errorResponse.getCode()), errorResponse.getMessage()));
        }
        return response;
    }

    // for diagnostic
    public String resultSetName() {
        return resultSetName;
    }

    public String diagnosticInfo() {
        String diagnosticInfo = " response status: ";
        if (!resultSetName.isEmpty()) {
            diagnosticInfo += "resultSetWire connected, Name = ";
            diagnosticInfo += resultSetName;
        } else {
            diagnosticInfo += "waiting for some response message";
        }
        return diagnosticInfo;
    }
}
