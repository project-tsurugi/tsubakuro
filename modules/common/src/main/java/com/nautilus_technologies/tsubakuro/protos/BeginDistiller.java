package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking ResponseProtos.Begin from ResponseProtos.Response.
 */
//FIXME: move to another module
public class BeginDistiller implements Distiller<ResponseProtos.Begin> {
    @Override
    public ResponseProtos.Begin distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.BEGIN.equals(response.getResponseCase())) {
	    LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
	    throw new IOException("response type is inconsistent with the request type");
	}
	return response.getBegin();
    }
}
