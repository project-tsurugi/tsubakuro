package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.tsurugidb.jogasaki.proto.SqlResponse;

/**
 * Interface Distiller classes which are innner static classes and intended to be used in FutureResponseImpl class.
 */
public interface Distiller<V> {
    V distill(SqlResponse.Response response) throws IOException, ServerException;
}
