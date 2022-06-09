package com.nautilus_technologies.tsubakuro.channel.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.util.ServerResource;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;

/**
 * SessionWire type.
 */
public interface SessionWire extends ServerResource {
    /**
     * Send a request to the SQL server.
     * @param request the request message encoded with protocol buffer
     * @param <V> type of response message, which should be assigned in accordance with the type of the request message
     * @param distiller the Distiller class that will work for the message to be received
     * @return FutureResponse contains prepared statement handle, where V
     * @throws IOException error occurred in request transfer
     */
    // FIXME: send(long servicdID, byte[]) -> FutureResponse<Response>
    <V> FutureResponse<V> send(long servicdID, SqlRequest.Request.Builder request, Distiller<V> distiller) throws IOException;

    /**
     * Send a query request to the SQL server.
     * @param request the request query message encoded with protocol buffer
     * @return Pair left of the Pair can be null if the query results in an error
     * @throws IOException error occurred in request transfer
     */
    // FIXME: remove this and use send()
    Pair<FutureResponse<SqlResponse.ExecuteQuery>, FutureResponse<SqlResponse.ResultOnly>> sendQuery(
            long servicdID, SqlRequest.Request.Builder request) throws IOException;

    /**
     * Receive the message corresponding to the given ResponseHandle from the SQL server
     * @param handle the handle of communication wire to receive incoming message
     * @return SqlResponse.Response the response message received from the SQL server
     * @throws IOException error occurred in responce receive
     */
    SqlResponse.Response receive(ResponseWireHandle handle) throws IOException;

    SqlResponse.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException;

    FutureResponse<? extends Response> send(long serviceID, byte[] request) throws IOException;
    FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) throws IOException;
    InputStream responseStream(ResponseWireHandle handle) throws IOException;
    InputStream responseStream(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException;

    /**
     * Treat the latest response message received as not having been read
     * @param handle the handle of communication wire to receive incoming message
     * @throws IOException error occurred in the unReceive operation
     */
    void unReceive(ResponseWireHandle handle) throws IOException;

    ResultSetWire createResultSetWire() throws IOException;

    /**
     * Closes this connection.
     * This method will be invoked one or more times.
     */
    @Override
    void close() throws IOException, InterruptedException;
}
