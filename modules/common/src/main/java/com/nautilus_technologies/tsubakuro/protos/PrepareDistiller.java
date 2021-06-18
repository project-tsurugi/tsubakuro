package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;

/**
 * Distiller class responsible for taking ResponseProtos.Prepare from ResponseProtos.Response.
 */
public class PrepareDistiller implements Distiller<ResponseProtos.Prepare> {
    public ResponseProtos.Prepare distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.PREPARE.equals(response.getResponseCase())) {
	    throw new IOException("response type is inconsistent with the request type");
	}				  
	return response.getPrepare();
    }
}
