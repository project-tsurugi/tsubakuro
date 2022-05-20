package com.tsurugidb.jogasaki.proto;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking SqlResponse.Prepare from SqlResponse.Response.
 */
//FIXME: move to another module
public class PrepareDistiller implements Distiller<SqlResponse.Prepare> {
    @Override
    public SqlResponse.Prepare distill(SqlResponse.Response response) throws IOException {
    if (!SqlResponse.Response.ResponseCase.PREPARE.equals(response.getResponseCase())) {
        LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
        throw new IOException("response type is inconsistent with the request type");
    }
    return response.getPrepare();
    }
}
