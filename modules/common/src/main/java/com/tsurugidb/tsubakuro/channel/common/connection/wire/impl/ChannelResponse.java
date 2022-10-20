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
    private final AtomicReference<SqlResponse.ExecuteQuery> head = new AtomicReference<>();
    private final AtomicReference<ResultSetWire> resultSet = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Lock lock = new ReentrantLock();
    private final Condition noSet = lock.newCondition();

    /**
     * Creates a new instance
     */
    public ChannelResponse() {
    }

    @Override
    public boolean isMainResponseReady() {
        return Objects.nonNull(main.get());
    }

    @Override
    public ByteBuffer waitForMainResponse() throws IOException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }

        lock.lock();
        try {
            while (Objects.isNull(main.get())) {
                noSet.await();
            }
            return main.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (Objects.nonNull(main.get())) {
            return main.get();
        }

        lock.lock();
        try {
            while (Objects.isNull(main.get())) {
                noSet.await();
            }
            return main.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public InputStream openSubResponse(String id) throws IOException, InterruptedException {
        if (id.equals(METADATA_CHANNEL_ID)) {
            return metadataChannel();
        } else if (id.equals(RELATION_CHANNEL_ID)) {
            return relationChannel();
        }
        throw new IOException("illegal SubResponse id");
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closed.set(true);
    }


    private InputStream relationChannel() throws IOException {
        return waitForResultSet().getByteBufferBackedInput();
    }

    private ResultSetWire waitForResultSet() throws IOException {
        if (Objects.nonNull(resultSet.get())) {
            return resultSet.get();
        }

        lock.lock();
        try {
            while (Objects.isNull(resultSet.get())) {
                noSet.await();
            }
            return resultSet.get();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
    }

//    private ResultSetWire waitForResultSet(long timeout, TimeUnit unit) throws IOException, TimeoutException {
//        if (Objects.nonNull(resultSet.get())) {
//            return resultSet.get();
//        }
//
//        lock.lock();
//        try {
//            while (Objects.isNull(resultSet.get())) {
//                noSet.await();
//            }
//            return resultSet.get();
//        } catch (InterruptedException e) {
//            throw new IOException(e);
//        } finally {
//            lock.unlock();
//        }
//    }

    private InputStream metadataChannel() throws IOException {
        if (Objects.isNull(resultSet.get())) {
            waitForResultSet();
        }
        var recordMeta = head.get().getRecordMeta();
        return new ByteBufferInputStream(ByteBuffer.wrap(recordMeta.toByteArray()));
    }

    // called from receiver thread
    public void setMainResponse(@Nonnull ByteBuffer response) {
        lock.lock();
        try {
            main.set(skipFrameworkHeader(response));
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }

    public void setResultSet(@Nonnull ByteBuffer response, @Nonnull ResultSetWire resultSetWire) throws IOException {
        var sqlResponse = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(skipFrameworkHeader(response)));
        var detailResponse = sqlResponse.getExecuteQuery();
        resultSetWire.connect(detailResponse.getName());
        lock.lock();
        try {
            head.set(detailResponse);
            resultSet.set(resultSetWire);
            noSet.signal();
        } finally {
            lock.unlock();
        }
    }

    private ByteBuffer skipFrameworkHeader(ByteBuffer response) {
        try {
            response.rewind();
            FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(response));
            return response;
        } catch (IOException e) {
            System.err.println(e);
            e.printStackTrace();
        }
        return null;  // FIXME
    }
}
