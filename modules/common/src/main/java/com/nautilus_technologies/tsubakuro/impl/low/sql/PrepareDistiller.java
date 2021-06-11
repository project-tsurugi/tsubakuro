package com.nautilus_technologies.tsubakuro.impl.low.sql;

import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Distiller class responsible for taking ResponseProtos.Prepare from ResponseProtos.Response.
 */
public final class PrepareDistiller implements Distiller<ResponseProtos.Prepare> {
    public ResponseProtos.Prepare distill(ResponseProtos.Response response) {
	return response.getPrepare();
    }
}
