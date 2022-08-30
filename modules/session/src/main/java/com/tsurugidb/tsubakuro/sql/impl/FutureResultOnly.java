package com.tsurugidb.tsubakuro.sql.impl;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.tsurugidb.tateyama.proto.SqlResponse;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.tsubakuro.exception.SqlServiceException;

public final class FutureResultOnly implements FutureResponse<Void> {
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

    public Void get() throws SqlServiceException {
        if (Objects.nonNull(message)) {
            if (SqlResponse.ResultOnly.ResultCase.ERROR.equals(message.getResultCase())) {
                var errorResponse = message.getError();
                throw new SqlServiceException(SqlServiceCode.valueOf(errorResponse.getStatus()), errorResponse.getDetail());
            }
            return null;
        }
        throw exception;
    }

    public Void get(long timeout, TimeUnit unit) throws SqlServiceException {
        return get();  // timeout control is unnecessary because the result is already available.
    }

    public boolean isDone() {
        return true;
    }

    public void close() {
    }
}
