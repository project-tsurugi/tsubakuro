package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Wire type.
 */
public interface Wire extends Closeable {
    /**
     * Send a request to the SQL server.
     @param request the request message encoded with protocol buffer
     @return FutureResponse<V> contains prepared statement handle, where V should be assigned in accordance with the type of the response message to be returned
    */
    <V> FutureResponse<V> send(RequestProtos.Request request, FutureResponse.Distiller<V> distiller) throws IOException;

    /**
     * Receive the message corresponding to the given ResponseHandle from the SQL server
     @param handle the data that contains information indicating the interface to receive incoming message
     @return ResponseProtos.Response the response message received from the SQL server
    */
    ResponseProtos.Response recv(ResponseHandle handle) throws IOException;
}
