package com.nautilus_technologies.tsubakuro.channel.common.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;

/**
 * FutureQueryResponseImpl type.
 */
// FIXME: remove SQL specific implementation
public class FutureQueryResponseImpl implements FutureResponse<SqlResponse.ExecuteQuery> {
    private final SessionWire sessionWireImpl;
    private ResponseWireHandle responseWireHandleImpl;
    private final AtomicBoolean isDone = new AtomicBoolean(false);

    /**
     * Class constructor, called from SessionWire that is connected to the SQL server.
     * @param sessionWireImpl the wireImpl class responsible for this communication
     */
    public FutureQueryResponseImpl(SessionWire sessionWireImpl) {
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
    public SqlResponse.ExecuteQuery get() throws IOException {
        if (Objects.isNull(responseWireHandleImpl)) {
            throw new IOException("request has not been send out");
        }
        var response = sessionWireImpl.receive(responseWireHandleImpl);
        if (SqlResponse.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
            return response.getExecuteQuery();
        }
        sessionWireImpl.unReceive(responseWireHandleImpl);
        return null;
    }

    @Override
    public SqlResponse.ExecuteQuery get(long timeout, TimeUnit unit) throws TimeoutException, IOException {
        if (Objects.isNull(responseWireHandleImpl)) {
            throw new IOException("request has not been send out");
        }
        var response = sessionWireImpl.receive(responseWireHandleImpl, timeout, unit);
        if (SqlResponse.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
            return response.getExecuteQuery();
        }
        sessionWireImpl.unReceive(responseWireHandleImpl);
        return null;
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        // FIXME: impl
    }
}
