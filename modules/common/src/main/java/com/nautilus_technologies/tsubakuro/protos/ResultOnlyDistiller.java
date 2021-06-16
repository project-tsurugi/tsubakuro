package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;

/**
 * Distiller class responsible for taking ResponseProtos.ResultOnly from ResponseProtos.Response.
 */
public class ResultOnlyDistiller implements Distiller<ResponseProtos.ResultOnly> {
    public ResponseProtos.ResultOnly distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
	    throw new IOException("response type is inconsistent with the request type");
	}				  
	return response.getResultOnly();
    }
}
