package com.nautilus_technologies.tsubakuro.protos;

import com.tsurugidb.jogasaki.proto.SqlResponse;

import java.io.IOException;
import org.slf4j.LoggerFactory;

/**
 * Distiller class responsible for taking SqlResponse.ExecuteQuery from SqlResponse.Response.
 */
//FIXME: move to another module
public class ExecuteQueryDistiller implements Distiller<SqlResponse.ExecuteQuery> {
    @Override
    public SqlResponse.ExecuteQuery distill(SqlResponse.Response response) throws IOException {
    if (!SqlResponse.Response.ResponseCase.EXECUTE_QUERY.equals(response.getResponseCase())) {
        LoggerFactory.getLogger(PrepareDistiller.class).error("response received is " + response);
        throw new IOException("response type is inconsistent with the request type");
    }
    return response.getExecuteQuery();
    }
}
