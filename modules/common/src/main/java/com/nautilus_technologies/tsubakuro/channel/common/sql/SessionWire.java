package com.nautilus_technologies.tsubakuro.channel.common.sql;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
    
/**
 * SessionWire type.
 */
public interface SessionWire extends Closeable {
    /**
     * Send a request to the SQL server.
     * @param request the request message encoded with protocol buffer
     * @param <V> type of response message, which should be assigned in accordance with the type of the request message
     * @param distiller the Distiller class that will work for the message to be received
     * @return FutureResponse contains prepared statement handle, where V 
     * @throws IOException error occurred in request transfer
     */
    <V> Future<V> send(RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException;

    /**
     * Send a query request to the SQL server.
     * @param request the request query message encoded with protocol buffer
     * @return Pair left of the Pair can be null if the query results in an error
     * @throws IOException error occurred in request transfer
     */
    Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> sendQuery(RequestProtos.Request.Builder request) throws IOException;

    /**
     * Receive the message corresponding to the given ResponseHandle from the SQL server
     * @param handle the handle of communication wire to receive incoming message
     * @return ResponseProtos.Response the response message received from the SQL server
     * @throws IOException error occurred in responce receive
     */
    ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException;

    ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException;

    /**
     * Treat the latest response message received as not having been read
     * @param handle the handle of communication wire to receive incoming message
     * @throws IOException error occurred in the unReceive operation
     */
    void unReceive(ResponseWireHandle handle) throws IOException;

    ResultSetWire createResultSetWire() throws IOException;
}
