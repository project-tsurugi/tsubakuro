package com.tsurugidb.tsubakuro.channel.stream.connection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.channel.stream.StreamLink;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * FutureStreamWireImpl type.
 */
public class FutureStreamWireImpl implements FutureResponse<Wire> {

    private final StreamLink streamLink;
    private final WireImpl wireImpl;
    private final FutureResponse<Long> futureSessionID;
    private final AtomicBoolean gotton = new AtomicBoolean();
    private final AtomicReference<Wire> result = new AtomicReference<>();
    private final Lock lock = new ReentrantLock();
    private boolean closed = false;

    FutureStreamWireImpl(StreamLink streamLink, WireImpl wireImpl, FutureResponse<Long> futureSessionID) {
        this.streamLink = streamLink;
        this.wireImpl = wireImpl;
        this.futureSessionID = futureSessionID;
    }

    @Override
    public Wire get() throws IOException, ServerException, InterruptedException {
        while (true) {
            var wire = result.get();
            if (wire != null) {
                return wire;
            }
            lock.lock();
            try {
                if (!gotton.getAndSet(true)) {
                    try {
                        wireImpl.setSessionID(futureSessionID.get());
                        result.set(wireImpl);
                        return wireImpl;
                    } catch (IOException | ServerException | InterruptedException e) {
                        closeInternal();
                        throw e;
                    }
                }
            } finally {
                lock.unlock();
            }
            if (closed) {
                throw new IOException("FutureStreamWireImpl is already closed");
            }
        }
    }

    @Override
    public Wire get(long timeout, TimeUnit unit) throws TimeoutException, IOException, ServerException, InterruptedException {
        while (true) {
            var wire = result.get();
            if (wire != null) {
                return wire;
            }
            if (lock.tryLock(timeout, unit)) {
                try {
                    if (!gotton.getAndSet(true)) {
                        try {
                            wireImpl.setSessionID(futureSessionID.get(timeout, unit));
                            result.set(wireImpl);
                            return wireImpl;
                        } catch (TimeoutException | IOException | ServerException | InterruptedException e) {
                            closeInternal();
                            throw e;
                        }
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                throw new TimeoutException("get() by another thread has not returned within the specifined time");
            }
            if (closed) {
                throw new IOException("FutureStreamWireImpl is already closed");
            }
        }
    }

    @Override
    public boolean isDone() {
        return result.get() != null;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (!gotton.getAndSet(true)) {
            closeInternal();
        }
    }

    private void closeInternal() throws IOException, InterruptedException, ServerException {
        Exception top = null;
        if (!closed) {
            closed = true;
            if (result.get() == null) {
                try {
                    futureSessionID.close();
                } catch (Exception e) {
                    top = e;
                } finally {
                    try {
                        streamLink.closeWithoutGet();
                    } catch (Exception e) {
                        if (top == null) {
                            top = e;
                        } else {
                            top.addSuppressed(e);
                        }
                    } finally {
                        try {
                            wireImpl.closeWithoutGet();
                        } catch (Exception e) {
                            if (top == null) {
                                top = e;
                            } else {
                                top.addSuppressed(e);
                            }
                        } finally {
                            if (top != null) {
                                if (top instanceof IOException) {
                                    throw (IOException) top;
                                }
                                if (top instanceof InterruptedException) {
                                    throw (InterruptedException) top;
                                }
                                if (top instanceof ServerException) {
                                    throw (ServerException) top;
                                }
                                throw new AssertionError(top);
                            }
                        }
                    }
                }
            }
        }
    }
}
