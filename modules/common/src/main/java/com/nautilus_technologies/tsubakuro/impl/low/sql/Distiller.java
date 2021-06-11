package com.nautilus_technologies.tsubakuro.impl.low.sql;

import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Interface Distiller classes which are innner static classes and intended to be used in FutureResponseImpl class.
 */
public interface Distiller<V> {
    V distill(ResponseProtos.Response response);
}
