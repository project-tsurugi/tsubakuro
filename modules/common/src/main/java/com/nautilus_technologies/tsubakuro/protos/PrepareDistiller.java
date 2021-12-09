package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking ResponseProtos.Prepare from ResponseProtos.Response.
 */
public class PrepareDistiller implements Distiller<ResponseProtos.Prepare> {
    public ResponseProtos.Prepare distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.PREPARE.equals(response.getResponseCase())) {
	    LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
	    throw new IOException("response type is inconsistent with the request type");
	}				  
	return response.getPrepare();
    }
}
