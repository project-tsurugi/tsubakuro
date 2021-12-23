package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * PreparedStatementImpl type.
 */
public class PreparedStatementImpl implements PreparedStatement {
    private long timeout;
    private TimeUnit unit;
    CommonProtos.PreparedStatement handle;
    private SessionLinkImpl sessionLinkImpl;
    
    public PreparedStatementImpl(CommonProtos.PreparedStatement handle, SessionLinkImpl sessionLinkImpl) {
	this.timeout = 0;
	this.handle = handle;
	this.sessionLinkImpl = sessionLinkImpl;
    }

    public CommonProtos.PreparedStatement getHandle() throws IOException {
	if (Objects.isNull(handle)) {
	    throw new IOException("already closed");
	}
	return handle;
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param timeout time length until the close operation timeout
     * @param unit unit of timeout
     */
    public void setCloseTimeout(long t, TimeUnit u) {
	timeout = t;
	unit = u;
    }
    
    /**
     * Close the PreparedStatementImpl
     */
    public void close() throws IOException {
	if (Objects.nonNull(handle)) {
	    try {
		var futureResponse = sessionLinkImpl.send(RequestProtos.DisposePreparedStatement.newBuilder().setPreparedStatementHandle(handle));
		var response = (timeout == 0) ? futureResponse.get() : futureResponse.get(timeout, unit);
		if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
		    throw new IOException(response.getError().getDetail());
		}
	    } catch (TimeoutException | InterruptedException | ExecutionException e) {
		throw new IOException(e);
	    }
	    handle = null;
	} else {
	    throw new IOException("already closed");
	}
    }
}
