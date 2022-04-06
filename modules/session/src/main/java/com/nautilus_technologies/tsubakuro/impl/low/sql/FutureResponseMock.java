package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.nio.file.Path;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * FutureResponseMock type.
 */
public class FutureResponseMock implements Future<ResponseProtos.ResultOnly> {
    private boolean isDone = false;
    private boolean isCancelled = false;
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

    public ResponseProtos.ResultOnly get() throws ExecutionException {
	boolean ng = false;

	if (success) {
	    return ResponseProtos.ResultOnly.newBuilder()
		.setSuccess(ResponseProtos.Success.newBuilder())
		.build();
	}
	return ResponseProtos.ResultOnly.newBuilder()
	    .setError(ResponseProtos.Error.newBuilder().setDetail("intentional fail for test purpose"))
	    .build();
    }

    public ResponseProtos.ResultOnly get(long timeout, TimeUnit unit) throws ExecutionException {
	return get();
    }
    public boolean isDone() {
	return isDone;
    }
    public boolean isCancelled() {
	return isCancelled;
    }
    public boolean cancel(boolean mayInterruptIfRunning) {
	isCancelled = true;
	isDone = true;
	return true;
    }
}
