package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {
    public static final String METADATA_CHANNEL_ID = "metadata";
    public static final String RELATION_CHANNEL_ID = "relation";

    public static final int CANCEL_STATUS_NO_SLOT = -1;
    public static final int CANCEL_STATUS_RESPONSE_ARRIVED = -2;
    public static final int CANCEL_STATUS_REQUESTING = -3;
    public static final int CANCEL_STATUS_REQUESTED = -4;
    private final AtomicInteger cancelStatus =  new AtomicInteger();
    private long cancelThreadId = 0;

    private final AtomicReference<ByteBuffer> main = new AtomicReference<>();
    private final AtomicReference<SqlResponse.ExecuteQuery> metadata = new AtomicReference<>();
    private final AtomicReference<ResultSetWire> resultSet = new AtomicReference<>();
    private final AtomicReference<Exception> exceptionMain = new AtomicReference<>();
    private final AtomicReference<Exception> exceptionResultSet = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Link link;
    private String resultSetName = "";  // for diagnostic

    /**
     * Creates a new instance
     * @param link the link object by which this ChannelResponse pulls a message from the SQL server
     * @param slot the slot number in the responseBox
     */
    public ChannelResponse(Link link, int slot) {
        this.link = link;
        this.cancelStatus.set(slot);
    }

    /**
     * Creates a new instance that is not associated with a slot in the responseBox
     * @param link the link object by which this ChannelResponse pulls a message from the SQL server
     */
    public ChannelResponse(Link link) {
        this.link = link;
        this.cancelStatus.set(CANCEL_STATUS_NO_SLOT);
    }

    @Override
    public boolean isMainResponseReady() {
        return (main.get() != null) || (exceptionMain.get() != null);
    }

    @Override
    public ByteBuffer waitForMainResponse() throws IOException {
        try {
            return waitForMainResponse(0, null);
        } catch (TimeoutException e) {  // TimeoutException won't be occuer when timeout is 0.
            throw new IOException(e);
        }
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        while (true) {
            var n = link.messageNumber();
            if (main.get() != null) {
                return main.get();
            }
            if (exceptionMain.get() != null) {
                var e = exceptionMain.get();
                throw new IOException(e.getMessage(), e);
            }
            link.pullMessage(n, timeout, unit);
        }
    }

    @Override
    public InputStream openSubResponse(String id) throws NoSuchElementException, IOException, InterruptedException {
        if (id.equals(METADATA_CHANNEL_ID)) {
            waitForResultSetOrMainResponse();
            return metadataChannel();
        } else if (id.equals(RELATION_CHANNEL_ID)) {
            waitForResultSetOrMainResponse();
            return relationChannel();
        }
        throw new NoSuchElementException("illegal SubResponse id");
    }

    @Override
    public InputStream openSubResponse(String id, long timeout, TimeUnit unit) throws NoSuchElementException, IOException, InterruptedException, TimeoutException {
        if (id.equals(METADATA_CHANNEL_ID)) {
            waitForResultSetOrMainResponse(timeout, unit);
            return metadataChannel();
        } else if (id.equals(RELATION_CHANNEL_ID)) {
            waitForResultSetOrMainResponse(timeout, unit);
            return relationChannel();
        }
        throw new NoSuchElementException("illegal SubResponse id");
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    /**
     * Assign slot number in the responseBox to this object.
     * @param slot the slot number in the responseBox
     * @return false when cancel has already took place
     */
    public boolean assignSlot(int slot) {
        while (true) {
            var expected = cancelStatus.get();
            if (expected == CANCEL_STATUS_NO_SLOT) {
                if (cancelStatus.compareAndSet(expected, slot)) {
                    return true;
                }
            }
            if (expected == CANCEL_STATUS_REQUESTED) {
                return false;
            }
        }
    }

    @Override
    public void cancel() throws IOException {
        while (true) {
            var expected = cancelStatus.get();
            if (expected >= 0) {
                if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_REQUESTING)) {
                    try {
                        cancelThreadId = Thread.currentThread().getId();
                        link.send(expected, CancelMessage.header(link.sessionId), CancelMessage.payload(), this);
                    } finally {
                        cancelThreadId = 0;
                        cancelStatus.set(CANCEL_STATUS_REQUESTED);
                        return;
                    }
                }
                continue;
            }
            if (cancelStatus.compareAndSet(CANCEL_STATUS_NO_SLOT, CANCEL_STATUS_REQUESTED)) {
                return;  // cancel before request send
            }
            if (expected == CANCEL_STATUS_RESPONSE_ARRIVED) {
                return;  // response has already arrived
            }
            if (expected == CANCEL_STATUS_REQUESTED) {
                return;  // cancel twice
            }
        }
    }

    public void cancelSuccessWithoutServerInteraction() {
        setMainResponse(new IOException("cancel has been conducted within tsubakuro"));
    }

    private void responseArrive() {
        if (cancelThreadId == Thread.currentThread().getId()) {
            return;
        }
        while (true) {
            var expected = cancelStatus.get();
            if (expected >= 0) {
                if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_RESPONSE_ARRIVED)) {
                    return;
                }
                continue;
            }
            if (expected == CANCEL_STATUS_REQUESTED) {
                cancelStatus.compareAndSet(expected, CANCEL_STATUS_RESPONSE_ARRIVED);
                return;  // Cancel operation is being executed at the same time. Either, REQUESTED or RESPONSE_ARRIVED, is OK.
            }
            if (expected == CANCEL_STATUS_RESPONSE_ARRIVED) {
                return;  // response arrives twice, implies some error.
            }
            // try again if status is CANCEL_STATUS_REQUESTING.
        }
    }

    private InputStream relationChannel() throws IOException, InterruptedException {
        if (resultSet.get() != null) {
            return resultSet.get().getByteBufferBackedInput();
        }
        if (exceptionResultSet.get() != null) {
            var e = exceptionResultSet.get();
            throw new IOException(e.getMessage(), e);
        }
        return null;
    }

    private InputStream metadataChannel() throws IOException, InterruptedException {
        if (metadata.get() != null) {
            var recordMeta = metadata.get().getRecordMeta();
            return new ByteBufferInputStream(ByteBuffer.wrap(recordMeta.toByteArray()));
        }
        if (exceptionResultSet.get() != null) {
            var e = exceptionResultSet.get();
            throw new IOException(e.getMessage(), e);
        }
        return null;
    }

    private void waitForResultSetOrMainResponse() throws IOException, InterruptedException {
        try {
            waitForResultSetOrMainResponse(0, null);
        } catch (TimeoutException e) {  // TimeoutException won't be occuer when timeout is 0.
            throw new IOException(e);
        }
    }

    private void waitForResultSetOrMainResponse(long timeout, TimeUnit unit) throws IOException, InterruptedException, TimeoutException {
        while (true) {
            var n = link.messageNumber();
            if (isResultSetReady() || (isMainResponseReady())) {
                return;
            }
            link.pullMessage(n, timeout, unit);
        }
    }

    private boolean isResultSetReady() {
        return (resultSet.get() != null) || (exceptionResultSet.get() != null);
    }

    // get call from a thread that has received the response
    void setMainResponse(@Nonnull ByteBuffer response) {
        Objects.requireNonNull(response);
        try {
            main.set(skipFrameworkHeader(response));
        } catch (IOException | ServerException e) {
            exceptionMain.set(e);
        }
    }
    public void setMainResponse(@Nonnull IOException exception) {
        Objects.requireNonNull(exception);
        exceptionMain.set(exception);
    }

    public void setResultSet(@Nonnull ByteBuffer response, @Nonnull ResultSetWire resultSetWire) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(resultSetWire);
        try {
            var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(skipFrameworkHeader(response)));
            var detailResponse = sqlResponse.getExecuteQuery();
            resultSetName = detailResponse.getName();
            resultSetWire.connect(resultSetName);

            metadata.set(detailResponse);
            resultSet.set(resultSetWire);
        } catch (IOException | ServerException e) {
            exceptionResultSet.set(e);
        }
    }

    private ByteBuffer skipFrameworkHeader(ByteBuffer response) throws IOException, ServerException {
        response.rewind();
        var header = FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(response));
        if (header.getPayloadType() == com.tsurugidb.framework.proto.FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS) {
            var errorResponse = com.tsurugidb.diagnostics.proto.Diagnostics.Record.parseDelimitedFrom(new ByteBufferInputStream(response));
            throw new CoreServiceException(CoreServiceCode.valueOf(errorResponse.getCode()), errorResponse.getMessage());
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
