package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.TimeUnit;

import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceException;

public final class FutureResultOnly implements FutureResponse<SqlResponse.ResultOnly> {
    /**
     * The service ID of SQL service ({@value}).
     */
    private SqlResponse.ResultOnly message;

    public FutureResultOnly() {
        message = SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder()).build();
    }
    public FutureResultOnly(SqlResponse.ResultOnly message) {
        this.message = message;
    }

    public SqlResponse.ResultOnly get() throws SqlServiceException {
        if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(message.getResultCase())) {
            var errorResponse = message.getError();
            throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
        }
        return message;
    }

    public SqlResponse.ResultOnly get(long timeout, TimeUnit unit) throws SqlServiceException {
        return get();
    }

    public boolean isDone() {
        return true;
    }

    public void close() {
    }
}
