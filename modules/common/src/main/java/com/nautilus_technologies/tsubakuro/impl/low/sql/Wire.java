package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.Future;
import java.io.Closeable;
import java.io.IOException;
import com.nautilus_technologies.tsubakuro.low.sql.SessionLink;
import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Wire type.
 */
public interface Wire extends Closeable {
    void close() throws IOException;

    <V> FutureResponse<V> send(RequestProtos.Request request, FutureResponse.Distiller<V> distiller) throws IOException;

    ResponseProtos.Response recv() throws IOException;
}
