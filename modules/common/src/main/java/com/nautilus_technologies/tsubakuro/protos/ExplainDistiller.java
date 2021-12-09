package com.nautilus_technologies.tsubakuro.protos;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking ResponseProtos.Explain from ResponseProtos.Response.
 */
public class ExplainDistiller implements Distiller<ResponseProtos.Explain> {
    public ResponseProtos.Explain distill(ResponseProtos.Response response) throws IOException {
	if (!ResponseProtos.Response.ResponseCase.EXPLAIN.equals(response.getResponseCase())) {
	    LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
	    throw new IOException("response type is inconsistent with the request type");
	}				  
	return response.getExplain();
    }
}
