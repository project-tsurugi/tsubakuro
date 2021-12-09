package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;

/**
 * Distiller class responsible for taking ResponseProtos.Explain from ResponseProtos.Response.
 */
public class ExplainDistiller implements Distiller<ResponseProtos.Explain> {
    public ResponseProtos.Explain distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.EXPLAIN.equals(response.getResponseCase())) {
	    throw new IOException("response type is inconsistent with the request type");
	}				  
	return response.getExplain();
    }
}
