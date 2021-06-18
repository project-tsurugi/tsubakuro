package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;

/**
 * Interface Distiller classes which are innner static classes and intended to be used in FutureResponseImpl class.
 */
public interface Distiller<V> {
    V distill(ResponseProtos.Response response) throws IOException;
}
