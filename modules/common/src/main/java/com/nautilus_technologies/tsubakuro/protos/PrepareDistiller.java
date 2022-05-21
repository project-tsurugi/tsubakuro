package com.nautilus_technologies.tsubakuro.protos;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking ResponseProtos.Prepare from ResponseProtos.Response.
 */
//FIXME: move to another module
public class PrepareDistiller implements Distiller<ResponseProtos.Prepare> {
    @Override
    public ResponseProtos.Prepare distill(ResponseProtos.Response response) throws IOException, ServerException {
		if (!ResponseProtos.Response.ResponseCase.PREPARE.equals(response.getResponseCase())) {
			LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
			throw new IOException("response type is inconsistent with the request type");
		}
		var detailResponse = response.getPrepare();
		if (ResponseProtos.Prepare.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
			var errorResponse = detailResponse.getError();
			throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
		}
		return detailResponse;
	}
}
