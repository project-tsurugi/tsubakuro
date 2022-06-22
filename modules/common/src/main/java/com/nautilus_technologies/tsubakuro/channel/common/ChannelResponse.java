package com.nautilus_technologies.tsubakuro.channel.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
// import java.text.MessageFormat;
// import java.util.ArrayList;
import java.util.Collection;
// import java.util.Collections;
// import java.util.Map;
// import java.util.NoSuchElementException;
import java.util.Objects;
// import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
// import com.nautilus_technologies.tsubakuro.util.ByteBufferByteBuffer;

/**
 * A simple implementation of {@link Response} which just returns payload data.
 */
public class ChannelResponse implements Response {

    private final SessionWire wire;
    private ResponseWireHandle handle;
    private boolean queryMode;
//    private final ByteBuffer main;

//    private final Map<String, ByteBuffer> subs;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Creates a new instance.
     * @param main the main response data
     * @param subMap map of sub response ID and its data
     */
//    public ChannelResponse(@Nonnull ByteBuffer main, @Nonnull Map<String, ByteBuffer> subMap) {
    public ChannelResponse(@Nonnull SessionWire wire) {
        Objects.requireNonNull(wire);
//        Objects.requireNonNull(subMap);
        this.wire = wire;
        this.handle = null;
        this.queryMode = false;
//        this.subs = new TreeMap<>();
//        for (var entry : subMap.entrySet()) {
//            this.subs.put(entry.getKey(), entry.getValue().duplicate());
//        }
    }

    /**
     * Creates a new instance, without any attached data.
     * @param main the main response data
     */
//    public ChannelResponse(@Nonnull ByteBuffer main) {
//        this(main, Collections.emptyMap());
//    }

    @Override
    public synchronized boolean isMainResponseReady() {
        return Objects.nonNull(handle);
    }

    @Override
    public synchronized ByteBuffer waitForMainResponse() throws IOException {
        if (isMainResponseReady()) {
            return wire.response(handle);
        }
        throw new IOException("response box is not available");  // FIXME arch. mismatch??
    }

    @Override
    public synchronized ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
        if (isMainResponseReady()) {
            return wire.response(handle, timeout, unit);
        }
        throw new IOException("response box is not available");  // FIXME arch. mismatch??
    }

    @Override
    public Collection<String> getSubResponseIds() throws IOException, ServerException, InterruptedException {
//        return new ArrayList<>(subs.keySet());
        return null;
    }

    @Override
    public InputStream openSubResponse(String id) throws IOException, ServerException, InterruptedException {
//        checkOpen();
//        var data = subs.remove(id);
//        if (data == null) {
//            throw new NoSuchElementException(id);
//        }
//        return new ByteBufferByteBuffer(data);
        return null;
    }

//    private void checkOpen() {
//        if (closed.get()) {
//            throw new IllegalStateException("response was already closed");
//        }
//    }

    @Override
    public void close() throws IOException, InterruptedException {
//        subs.clear();
        closed.set(true);
    }

//    @Override
//    public String toString() {
//        return MessageFormat.format(
//                "ChannelResponse(main={0}, sub={1})",
//                main.remaining(),
//                subs.keySet());
//                main.remaining()
//            "ChannelResponse"  // FIXME can not pirnt the contents of ByteBuffer??
//        );
//    }

    @Override
    public synchronized ResponseWireHandle responseWireHandle() {
        return handle;
    }

    @Override
    public synchronized void setQueryMode() {
        queryMode = true;
        if (Objects.nonNull(handle)) {
            wire.setQueryMode(handle);
        }
    }

    public synchronized void setResponseHandle(ResponseWireHandle h) {
        handle = h;
        if (queryMode) {
            wire.setQueryMode(handle);
        }
    }
}
