package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Objects;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;

/**
 * PreparedStatementImpl type.
 */
public class PreparedStatementImpl implements PreparedStatement {
    CommonProtos.PreparedStatement handle;
    private SessionLinkImpl sessionLinkImpl;
    
    public PreparedStatementImpl(CommonProtos.PreparedStatement handle, SessionLinkImpl sessionLinkImpl) {
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
     * Close the PreparedStatementImpl
     */
    public void close() throws IOException {
	if (Objects.nonNull(handle)) {
	    try {
		var response = sessionLinkImpl.send(RequestProtos.DisposePreparedStatement.newBuilder().setPreparedStatementHandle(handle)).get();
		if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
		    throw new IOException(response.getError().getDetail());
		}
	    } catch (InterruptedException e) {
		throw new IOException(e);
	    } catch (ExecutionException e) {
		throw new IOException(e);
	    }
	    handle = null;
	} else {
	    throw new IOException("already closed");
	}
    }
}
