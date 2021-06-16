package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;

/**
 * Distiller class responsible for taking ResponseProtos.Begin from ResponseProtos.Response.
 */
public class BeginDistiller implements Distiller<ResponseProtos.Begin> {
    public ResponseProtos.Begin distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.BEGIN.equals(response.getResponseCase())) {
	    throw new IOException("response type is inconsistent with the request type");
	}				  
	return response.getBegin();
    }
}
