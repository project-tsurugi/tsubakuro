package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceException;

public final class FutureResultOnly implements FutureResponse<SqlResponse.ResultOnly> {
    /**
     * The service ID of SQL service ({@value}).
     */
    private final SqlResponse.ResultOnly message;
    private final SqlServiceException exception;

    public FutureResultOnly() {
        message = SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder()).build();
        this.exception = null;
    }
    public FutureResultOnly(@Nonnull SqlResponse.ResultOnly message) {
        Objects.requireNonNull(message);
        this.message = message;
        this.exception = null;
    }
    public FutureResultOnly(@Nonnull SqlServiceException exception) {
        Objects.requireNonNull(exception);
        this.exception = exception;
        this.message = null;
    }

    public SqlResponse.ResultOnly get() throws SqlServiceException {
        if (Objects.nonNull(message)) {
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(message.getResultCase())) {
                var errorResponse = message.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return message;
        }
        throw exception;
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
