package com.nautilus_technologies.tsubakuro.impl.low.sql;

import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Interface Distiller classes which are innner static classes and intended to be used in FutureResponseImpl class.
 */
public interface Distiller<V> {
    V distill(ResponseProtos.Response response);
}

/**
 * Distiller class responsible for taking ResponseProtos.Prepare from ResponseProtos.Response.
 */
class PrepareDistiller implements Distiller<ResponseProtos.Prepare> {
    public ResponseProtos.Prepare distill(ResponseProtos.Response response) {
	return response.getPrepare();
    }
}

/**
 * Distiller class responsible for taking ResponseProtos.ResultOnly from ResponseProtos.Response.
 */
class ResultOnlyDistiller implements Distiller<ResponseProtos.ResultOnly> {
    public ResponseProtos.ResultOnly distill(ResponseProtos.Response response) {
	return response.getResultOnly();
    }
}

/**
 * Distiller class responsible for taking ResponseProtos.ExecuteQuery from ResponseProtos.Response.
 */
class ExecuteQueryDistiller implements Distiller<ResponseProtos.ExecuteQuery> {
    public ResponseProtos.ExecuteQuery distill(ResponseProtos.Response response) {
	return response.getExecuteQuery();
    }
}

/**
 * Distiller class responsible for taking ResponseProtos.Begin from ResponseProtos.Response.
 */
class BeginDistiller implements Distiller<ResponseProtos.Begin> {
    public ResponseProtos.Begin distill(ResponseProtos.Response response) {
	return response.getBegin();
    }
}
