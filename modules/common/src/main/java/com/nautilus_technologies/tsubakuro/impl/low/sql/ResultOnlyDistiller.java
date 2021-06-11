package com.nautilus_technologies.tsubakuro.impl.low.sql;

import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Distiller class responsible for taking ResponseProtos.ResultOnly from ResponseProtos.Response.
 */
public final class ResultOnlyDistiller implements Distiller<ResponseProtos.ResultOnly> {
    public ResponseProtos.ResultOnly distill(ResponseProtos.Response response) {
	return response.getResultOnly();
    }
}
