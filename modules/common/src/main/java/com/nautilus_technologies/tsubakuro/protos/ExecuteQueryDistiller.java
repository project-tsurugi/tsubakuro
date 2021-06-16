package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;

/**
 * Distiller class responsible for taking ResponseProtos.ExecuteQuery from ResponseProtos.Response.
 */
public class ExecuteQueryDistiller implements Distiller<ResponseProtos.ExecuteQuery> {
    public ResponseProtos.ExecuteQuery distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
	    throw new IOException("response type is inconsistent with the request type");
	}				  
	return response.getExecuteQuery();
    }
}
