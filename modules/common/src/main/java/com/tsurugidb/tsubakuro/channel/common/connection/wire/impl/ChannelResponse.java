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
package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.tsurugidb.diagnostics.proto.Diagnostics;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.Pair;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {
    public static final String METADATA_CHANNEL_ID = "metadata";
    public static final String RELATION_CHANNEL_ID = "relation";

    // cancelStatus >= 0 means the request has been sent to the server
    // cancelStatus < 0 means the following state
    private final AtomicInteger cancelStatus = new AtomicInteger();

    // the request is in the queue
    public static final int CANCEL_STATUS_NO_SLOT = -1;
    // the response for the requet including normal request has been alived
    public static final int CANCEL_STATUS_RESPONSE_ARRIVED = -2;
    // cancel request is sending now
    public static final int CANCEL_STATUS_CANCEL_SENDING = -3;
    // cancel request has been sent out
    public static final int CANCEL_STATUS_CANCEL_SENT = -4;
    // the request is to be sent
    public static final int CANCEL_STATUS_REQUEST_SNEDING = -5;
    // the request is not sent out
    public static final int CANCEL_STATUS_REQUEST_DO_NOT_SEND = -6;
    // the request is not sent out
    public static final int CANCEL_STATUS_CANCEL_DO_NOT_SEND = -7;
    // the request is not sent out
    public static final int CANCEL_STATUS_ALREADY_RECEIVED = -8;

    private long cancelThreadId = 0;

    private final AtomicReference<ByteBuffer> main = new AtomicReference<>();
    private final AtomicReference<SqlResponse.ExecuteQuery> metadata = new AtomicReference<>();
    private final AtomicReference<ResultSetWire> resultSetWire = new AtomicReference<>();
    private final AtomicReference<Exception> exceptionMain = new AtomicReference<>();
    private final AtomicReference<Exception> exceptionResultSet = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final Link link;
    private String resultSetName = ""; // for diagnostic
    private final ConcurrentHashMap<String, Pair<String, Boolean>> blobs = new ConcurrentHashMap<>();

    /**
     * An exception notifying that the request has been canceled.
     */
    public static class AlreadyCanceledException extends IOException {
        AlreadyCanceledException() {
        }
    }

    /**
     * A file input stream class with file path return functionality.
     */
    public static class FileInputStreamWithPath extends FileInputStream {
        private final String path;

        /**
         * Creates a new instance
         * @param path the file path
         * @throws IOException the file indicated by path does not exist
         */
        FileInputStreamWithPath(String path) throws IOException {
            super(path);
            this.path = path;
        }

        /**
         * Returns a Path of the file corresponding to the FileInputStream.
         * @return a Path of the file
         */
        public Path path() {
            return Paths.get(path);
        }
    }

    /**
     * Creates a new instance
     *
     * @param link the link object by which this ChannelResponse pulls a message from the SQL server
     * @param slot the slot number in the responseBox
     */
    public ChannelResponse(Link link, int slot) {
        this.link = link;
        this.cancelStatus.set(CANCEL_STATUS_REQUEST_SNEDING);
    }

    /**
     * Creates a new instance that is not associated with a slot in the responseBox
     *
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
    public ByteBuffer waitForMainResponse() throws IOException, ServerException {
        try {
            return waitForMainResponse(0, null);
        } catch (TimeoutException e) { // TimeoutException won't be occur when timeout is 0.
            throw new ResponseTimeoutException(e.getMessage(), e);
        }
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, ServerException, TimeoutException {
        while (true) {
            var n = link.messageNumber();
            if (main.get() != null) {
                return main.get();
            }
            var e = exceptionMain.get();
            if (e != null) {
                if (canceled.get() && e instanceof CoreServiceException) {
                    if (((CoreServiceException) e).getDiagnosticCode() == CoreServiceCode.OPERATION_CANCELED) {
                        throw new AlreadyCanceledException();
                    }
                }
                wrapAndThrow(e);
                throw new IOException(e.getMessage(), e);
            }
            link.pullMessage(n, timeout, unit);
        }
    }

    void wrapAndThrow(Exception e) throws IOException, ServerException {
        if (e instanceof CoreServiceException) {
            throw ((CoreServiceException) e).newException();
        }
        if (e instanceof TimeoutException) {
            throw new ResponseTimeoutException(e.getMessage(), e);
        }
    }

    @Override
    public InputStream openSubResponse(String id) throws NoSuchElementException, IOException, InterruptedException, ServerException {
        if (id.equals(METADATA_CHANNEL_ID)) {
            waitForResultSetOrMainResponse();
            return metadataChannel();
        } else if (id.equals(RELATION_CHANNEL_ID)) {
            waitForResultSetOrMainResponse();
            return relationChannel();
        } else {
            waitForMainResponse();
            return returnsBlob(id);
        }
    }

    @Override
    public InputStream openSubResponse(String id, long timeout, TimeUnit unit) throws NoSuchElementException, IOException, InterruptedException, ServerException, TimeoutException {
        if (id.equals(METADATA_CHANNEL_ID)) {
            waitForResultSetOrMainResponse(timeout, unit);
            return metadataChannel();
        } else if (id.equals(RELATION_CHANNEL_ID)) {
            waitForResultSetOrMainResponse(timeout, unit);
            return relationChannel();
        } else {
            waitForMainResponse(timeout, unit);
            return returnsBlob(id);
        }
    }

    private InputStream returnsBlob(String id) throws NoSuchElementException, IOException {
        var entry = blobs.get(id);
        if (entry != null) {
            var path = entry.getLeft();
            if (path != null) {
                var filePath = Paths.get(path);
                if (Files.notExists(filePath)) {
                    throw new NoSuchFileException("client failed to receive BLOB file in privileged mode: {NoSuchFile:" + filePath.toString() + "}");
                }
                if (!Files.isReadable(filePath)) {
                    throw new AccessDeniedException("client failed to receive BLOB file in privileged mode: {CannotRead: " + filePath.toString() + "}");
                }
                return new FileInputStreamWithPath(path);
            }
        }
        throw new NoSuchElementException("client failed to receive BLOB file in privileged mode: {illegal SubResponse id: " + id + "}");
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }

    /**
     * Check if the request is ready to be sent.
     *
     * @param slot the slot number in the responseBox
     * @return false when cancel has already took place
     */
    boolean canAssignSlot() {
        while (true) {
            var expected = cancelStatus.get();
            if (expected == CANCEL_STATUS_NO_SLOT) {
                if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_REQUEST_SNEDING)) {
                    return true;
                }
            }
            if (expected == CANCEL_STATUS_CANCEL_SENT) {
                exceptionMain.set(new CoreServiceException(CoreServiceCode.valueOf(Diagnostics.Code.OPERATION_CANCELED), "The operation was canceled before the request was sent to the server"));
                return false;
            }
        }
    }

    /**
     * set cancelStatus and slot to the slot number given.
     * This method is called after the request has been sent to the server.
     *
     * @param slot the slot number in the responseBox
     */
    void finishAssignSlot(int slot) {
        while (true) {
            var expected = cancelStatus.get();
            if (expected ==  CANCEL_STATUS_ALREADY_RECEIVED) {
                if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_RESPONSE_ARRIVED)) {
                    return;
                }
                continue;
            }
            if (expected !=  CANCEL_STATUS_REQUEST_SNEDING && expected != CANCEL_STATUS_REQUEST_DO_NOT_SEND) {
                throw new AssertionError("request has not been sent, cancelStatus = " + cancelStatus.get());
            }
            if (cancelStatus.compareAndSet(expected, slot)) {
                return;
            }
        }
    }

    @Override
    public void cancel() throws IOException {
        canceled.set(true);
        while (true) {
            var expected = cancelStatus.get();
            if (expected >= 0) {
                if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_CANCEL_SENDING)) {
                    try {
                        cancelThreadId = Thread.currentThread().getId();
                        link.sendInternal(expected, CancelMessage.header(link.sessionId), CancelMessage.payload(), this);
                    } finally {
                        cancelThreadId = 0;
                        cancelStatus.set(CANCEL_STATUS_CANCEL_SENT);
                    }
                    return;
                }
                continue;
            }
            if (cancelStatus.compareAndSet(CANCEL_STATUS_NO_SLOT, CANCEL_STATUS_CANCEL_SENT)) {
                return; // cancel before request send
            }
            if (expected == CANCEL_STATUS_RESPONSE_ARRIVED) {
                return; // response has already arrived
            }
            if (expected == CANCEL_STATUS_CANCEL_SENT) {
                return; // cancel twice
            }
        }
    }

    public void cancelSuccessWithoutServerInteraction() {
        exceptionMain.set(new CoreServiceException(CoreServiceCode.valueOf(Diagnostics.Code.OPERATION_CANCELED), "The operation was canceled before the request was sent to the server"));
    }

    private void responseArrive(boolean received) {
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
            switch (expected) {
                case CANCEL_STATUS_REQUEST_SNEDING:
                    if (!received) {
                        if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_REQUEST_DO_NOT_SEND)) {
                            return;
                        }
                        break;
                    }
                    if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_ALREADY_RECEIVED)) {
                        return;
                    }
                    break;
                case CANCEL_STATUS_REQUEST_DO_NOT_SEND:
                case CANCEL_STATUS_NO_SLOT:
                    if (!received) {
                        return;
                    }
                    throw new AssertionError("response returned even though the request was not sent, state: " + expected);
                case CANCEL_STATUS_CANCEL_SENDING:
                    continue;  // try again if status is CANCEL_STATUS_CANCEL_SENDING.
                case CANCEL_STATUS_CANCEL_SENT:
                    if (cancelStatus.compareAndSet(expected, CANCEL_STATUS_RESPONSE_ARRIVED)) {
                        return; // Cancel operation is being executed at the same time. Either, REQUESTED or RESPONSE_ARRIVED, is OK.
                    }
                    break;
                case CANCEL_STATUS_RESPONSE_ARRIVED:
                    if (!received) {
                        return;
                    }
                    throw new AssertionError("response arrived twice, implies some error");
                case CANCEL_STATUS_CANCEL_DO_NOT_SEND:
                    if (!received) {
                        return;
                    }
                    throw new AssertionError("CANCEL_STATUS_CANCEL_DO_NOT_SEND should not appear here");
                default:
                    throw new AssertionError("illegal CANCEL_STATUS: " + expected);
            }
        }
    }

    private InputStream relationChannel() throws IOException, InterruptedException, ServerException {
        if (resultSetWire.get() != null) {
            return resultSetWire.get().getByteBufferBackedInput();
        }
        var e = exceptionResultSet.get();
        if (e != null) {
            wrapAndThrow(e);
            throw new IOException(e.getMessage(), e);
        }
        return new ByteArrayInputStream(new byte[0]);
    }

    private InputStream metadataChannel() throws IOException, InterruptedException, ServerException {
        if (metadata.get() != null) {
            var recordMeta = metadata.get().getRecordMeta();
            return new ByteBufferInputStream(ByteBuffer.wrap(recordMeta.toByteArray()));
        }
        var e = exceptionResultSet.get();
        if (e != null) {
            wrapAndThrow(e);
            throw new IOException(e.getMessage(), e);
        }
        return new ByteArrayInputStream(new byte[0]);
    }

    private void waitForResultSetOrMainResponse() throws IOException, InterruptedException {
        try {
            waitForResultSetOrMainResponse(0, null);
        } catch (TimeoutException e) { // TimeoutException won't be occur when timeout is 0.
            throw new ResponseTimeoutException(e.getMessage(), e);
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
        return (resultSetWire.get() != null) || (exceptionResultSet.get() != null);
    }

    // get call from a thread that has received the response
    void setMainResponse(@Nonnull ByteBuffer response) {
        Objects.requireNonNull(response);
        responseArrive(true);
        try {
            main.set(skipFrameworkHeader(response));
        } catch (IOException | CoreServiceException e) {
            exceptionMain.set(e);
        }
    }

    public void setMainResponse(@Nonnull IOException exception) {
        Objects.requireNonNull(exception);
        var e = exceptionMain.get();
        if (e != null) {
            e.addSuppressed(exception);
            return;
        }
        responseArrive(false);
        exceptionMain.set(exception);
    }

    public void setResultSet(@Nonnull ByteBuffer response, @Nonnull ResultSetWire rsw) {
        Objects.requireNonNull(response);
        Objects.requireNonNull(rsw);
        try {
            var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(skipFrameworkHeader(response)));
            var detailResponse = sqlResponse.getExecuteQuery();
            resultSetName = detailResponse.getName();
            rsw.connect(resultSetName);

            metadata.set(detailResponse);
            resultSetWire.set(rsw);
        } catch (IOException | CoreServiceException e) {
            exceptionResultSet.set(e);
        }
    }

    private ByteBuffer skipFrameworkHeader(ByteBuffer response) throws IOException, CoreServiceException {
        response.rewind();
        var header = FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(response));
        if (header.getPayloadType() == com.tsurugidb.framework.proto.FrameworkResponse.Header.PayloadType.SERVER_DIAGNOSTICS) {
            var errorResponse = com.tsurugidb.diagnostics.proto.Diagnostics.Record.parseDelimitedFrom(new ByteBufferInputStream(response));
            throw new CoreServiceException(CoreServiceCode.valueOf(errorResponse.getCode()), errorResponse.getMessage());
        }
        if (header.hasBlobs()) {
            for (var e : header.getBlobs().getBlobsList()) {
                blobs.put(e.getChannelName(), Pair.of(e.getPath(), e.getTemporary()));
            }
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
