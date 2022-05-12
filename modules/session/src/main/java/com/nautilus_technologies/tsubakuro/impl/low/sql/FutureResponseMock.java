package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;

/**
 * FutureResponseMock type.
 */
// FIXME: remove mock code
public class FutureResponseMock extends AbstractFutureResponse<ResponseProtos.ResultOnly> {
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
            return ResponseProtos.ResultOnly.newBuilder()
                    .setSuccess(ResponseProtos.Success.newBuilder())
                    .build();
        }
        return ResponseProtos.ResultOnly.newBuilder()
                .setError(ResponseProtos.Error.newBuilder().setDetail("intentional fail for test purpose"))
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
