package com.nautilus_technologies.tsubakuro.impl.low.sql;

import com.nautilus_technologies.tsubakuro.low.sql.ResponseProtos;

/**
 * Distiller class responsible for taking ResponseProtos.ExecuteQuery from ResponseProtos.Response.
 */
public final class ExecuteQueryDistiller implements Distiller<ResponseProtos.ExecuteQuery> {
    public ResponseProtos.ExecuteQuery distill(ResponseProtos.Response response) {
	return response.getExecuteQuery();
    }
}
