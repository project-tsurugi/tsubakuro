package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking ResponseProtos.ResultOnly from ResponseProtos.Response.
 */
//FIXME: move to another module
public class ResultOnlyDistiller implements Distiller<ResponseProtos.ResultOnly> {
    @Override
    public ResponseProtos.ResultOnly distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
	    LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
	    throw new IOException("response type is inconsistent with the request type");
	}
	return response.getResultOnly();
    }
}
