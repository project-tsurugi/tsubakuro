package com.nautilus_technologies.tsubakuro.channel.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Lang;

/**
 * FutureInputStream type.
 */
public class FutureInputStream implements FutureResponse<InputStream> {
    private final SessionWire sessionWireImpl;
    private ResponseWireHandle responseWireHandleImpl;

    private final AtomicBoolean isDone = new AtomicBoolean(true);

    /**
     * Class constructor, called from SessionWire that is connected to the SQL server.
     * @param sessionWireImpl the wireImpl class responsible for this communication
     * @param distiller the Distiller class that will work for the message to be received
     */
    public FutureInputStream(SessionWire sessionWireImpl) {
        this.sessionWireImpl = sessionWireImpl;
    }

    /**
     * Set responseWireHandle throuth which responses from the SQL server will be sent to this object.
     * @param handle the handle indicating the responseWire by which a response message is to be transferred
     */
    public void setResponseHandle(ResponseWireHandle handle) {
        responseWireHandleImpl = handle;
    }

    /**
     * get the message received from the SQL server.
     */
    @Override
    public InputStream get() throws IOException {
        if (Objects.isNull(responseWireHandleImpl)) {
            throw new IOException("request has not been send out");
        }
        var result = sessionWireImpl.responseStream(responseWireHandleImpl);
        isDone.set(true);
        return result;
    }

    @Override
    public InputStream get(long timeout, TimeUnit unit) throws TimeoutException, IOException {
        if (Objects.isNull(responseWireHandleImpl)) {
            throw new IOException("request has not been send out");
        }
        var result = sessionWireImpl.responseStream(responseWireHandleImpl, timeout, unit);
        isDone.set(true);
        return result;
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        // FIXME impl
        Lang.pass();
    }
}
