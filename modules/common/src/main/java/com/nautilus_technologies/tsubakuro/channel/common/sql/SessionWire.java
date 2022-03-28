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
     @param request the request message encoded with protocol buffer
     @return FutureResponse<V> contains prepared statement handle, where V should be assigned in accordance with the type of the response message to be returned
    */
    <V> Future<V> send(RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException;

    /**
     * Send a query request to the SQL server.
     @param request the request query message encoded with protocol buffer
     @return Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> left of the Pair can be null if the query results in an error
    */
    Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> sendQuery(RequestProtos.Request.Builder request) throws IOException;

    /**
     * Receive the message corresponding to the given ResponseHandle from the SQL server
     @param wire the information of communication wire to receive incoming message
     @return ResponseProtos.Response the response message received from the SQL server
    */
    ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException;

    ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException;

    /**
     * Treat the latest response message received as not having been read
     */
    void unReceive(ResponseWireHandle handle) throws IOException;

    ResultSetWire createResultSetWire() throws IOException;
}