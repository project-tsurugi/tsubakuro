package com.nautilus_technologies.tsubakuro.impl.low.sql;

import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Distiller class responsible for taking ResponseProtos.Begin from ResponseProtos.Response.
 */
public final class BeginDistiller implements Distiller<ResponseProtos.Begin> {
    public ResponseProtos.Begin distill(ResponseProtos.Response response) {
	return response.getBegin();
    }
}
