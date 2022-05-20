package com.tsurugidb.jogasaki.proto;

import java.io.IOException;

/**
 * Interface Distiller classes which are innner static classes and intended to be used in FutureResponseImpl class.
 */
public interface Distiller<V> {
    V distill(SqlResponse.Response response) throws IOException;
}
