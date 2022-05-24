package com.tsurugidb.jogasaki.proto;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceException;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking SqlResponse.Begin from SqlResponse.Response.
 */
//FIXME: move to another module
public class BeginDistiller implements Distiller<SqlResponse.Begin> {
    @Override
    public SqlResponse.Begin distill(SqlResponse.Response response) throws IOException, ServerException {
		if (!SqlResponse.Response.ResponseCase.BEGIN.equals(response.getResponseCase())) {
			LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
			throw new IOException("response type is inconsistent with the request type");
		}
		var detailResponse = response.getBegin();
		if (SqlResponse.Begin.ResultCase.ERROR.equals(detailResponse.getResultCase())) {
            var errorResponse = detailResponse.getError();
			throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
		}
		return detailResponse;
    }
}
