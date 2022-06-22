package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.concurrent.TimeUnit;

import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

public final class FutureResultOnlySuccess implements FutureResponse<SqlResponse.ResultOnly> {
    /**
     * The service ID of SQL service ({@value}).
     */
    public static final SqlResponse.ResultOnly MESSAGE = SqlResponse.ResultOnly.newBuilder().setSuccess(SqlResponse.Success.newBuilder()).build();

    public FutureResultOnlySuccess() {
    }

    public SqlResponse.ResultOnly get() {
        return MESSAGE;
    }

    public SqlResponse.ResultOnly get(long timeout, TimeUnit unit) {
        return MESSAGE;
    }

    public boolean isDone() {
        return true;
    }

    public void close() {
    }
}