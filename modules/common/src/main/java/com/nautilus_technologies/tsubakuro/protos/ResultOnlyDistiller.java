package com.tsurugidb.jogasaki.proto;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking SqlResponse.ResultOnly from SqlResponse.Response.
 */
//FIXME: move to another module
public class ResultOnlyDistiller implements Distiller<SqlResponse.ResultOnly> {
    @Override
    public SqlResponse.ResultOnly distill(SqlResponse.Response response) throws IOException {
    if (!SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(response.getResponseCase())) {
        LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
        throw new IOException("response type is inconsistent with the request type");
    }
    return response.getResultOnly();
    }
}
