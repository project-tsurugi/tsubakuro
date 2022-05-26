package com.nautilus_technologies.tsubakuro.protos;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.jogasaki.proto.SqlResponse;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking SqlResponse.ResultOnly from SqlResponse.Response.
 */
//FIXME: move to another module
public class ResultOnlyDistiller implements Distiller<SqlResponse.ResultOnly> {
    @Override
    public SqlResponse.ResultOnly distill(SqlResponse.Response response) throws IOException, ServerException {
		if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
			LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
			throw new IOException("response type is inconsistent with the request type");
		}
		var detailResponse = response.getResultOnly();
		if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
			var errorResponse = detailResponse.getError();
			throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
		}
		return detailResponse;
	}
}
