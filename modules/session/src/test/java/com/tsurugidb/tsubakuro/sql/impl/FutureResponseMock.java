/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.sql.proto.SqlResponse.ResultOnly;

/**
 * FutureResponseMock type.
 */
// FIXME: remove mock code
public class FutureResponseMock extends AbstractFutureResponse<SqlResponse.ResultOnly> {
    private boolean success;

    public FutureResponseMock(Collection<? extends Path> files) {
        for (Path file : files) {
            if (file.toString().contains("NG")) {
                this.success = false;
                return;
            }
        }
        this.success = true;
    }

    public FutureResponseMock(boolean success) {
        this.success = success;
    }

    @Override
    protected ResultOnly getInternal() throws IOException, ServerException, InterruptedException {
        if (success) {
            return SqlResponse.ResultOnly.newBuilder()
                    .setSuccess(SqlResponse.Success.newBuilder())
                    .build();
        }
        return SqlResponse.ResultOnly.newBuilder()
                .setError(SqlResponse.Error.newBuilder().setDetail("intentional fail for test purpose"))
                .build();
    }

    @Override
    protected ResultOnly getInternal(long timeout, TimeUnit unit)
            throws IOException, ServerException, InterruptedException, TimeoutException {
        return get();
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
    }
}
