package com.nautilus_technologies.tsubakuro.channel.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;

/**
 * SessionWire type.
 */
public interface SessionWire extends ServerResource {

    FutureResponse<? extends Response> send(long serviceID, byte[] request) throws IOException;
    FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) throws IOException;

    /**
     * Receive the message corresponding to the given ResponseHandle from the SQL server
     * @param handle the handle of communication wire to receive incoming message
     * @return SqlResponse.Response the response message received from the SQL server
     * @throws IOException error occurred in responce receive
     */
    ByteBuffer response(ResponseWireHandle handle) throws IOException;
    ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException;

    /**
     * Set to receive a Query type response by response box
     */
    void setQueryMode(ResponseWireHandle handle);

    /**
     * release the message in the response box
     * @param handle the handle to the response box
    */
    void release(ResponseWireHandle handle);

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return ResultSetWireImpl
    */
    ResultSetWire createResultSetWire() throws IOException;

    /**
     * Closes this connection.
     * This method will be invoked one or more times.
     */
    @Override
    void close() throws IOException, InterruptedException;
}
