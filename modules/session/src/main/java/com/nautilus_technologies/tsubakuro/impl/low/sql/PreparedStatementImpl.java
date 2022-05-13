package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.nautilus_technologies.tsubakuro.low.sql.PreparedStatement;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

/**
 * PreparedStatementImpl type.
 */
public class PreparedStatementImpl implements PreparedStatement {
    static final Logger LOG = LoggerFactory.getLogger(PreparedStatementImpl.class);

    private long timeout = 0;
    private TimeUnit unit;
    CommonProtos.PreparedStatement handle;
    private SessionLinkImpl sessionLinkImpl;

    public PreparedStatementImpl(CommonProtos.PreparedStatement handle, SessionLinkImpl sessionLinkImpl) {
        this.handle = handle;
        this.sessionLinkImpl = sessionLinkImpl;
        this.sessionLinkImpl.add(this);
    }

    public CommonProtos.PreparedStatement getHandle() throws IOException {
        if (Objects.isNull(handle)) {
            throw new IOException("already closed");
        }
        return handle;
    }

    @Override
    public boolean hasResultRecords() {
        return handle.getHasResultRecords();
    }

    /**
     * set timeout to close(), which won't timeout if this is not performed.
     * @param t time length until the close operation timeout
     * @param u unit of timeout
     */
    @Override
    public void setCloseTimeout(long t, TimeUnit u) {
        timeout = t;
        unit = u;
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        if (Objects.nonNull(handle) && Objects.nonNull(sessionLinkImpl)) {
            try {
                var futureResponse = sessionLinkImpl.send(RequestProtos.DisposePreparedStatement.newBuilder().setPreparedStatementHandle(handle));
                var response = (timeout == 0) ? futureResponse.get() : futureResponse.get(timeout, unit);
                if (ResponseProtos.ResultOnly.ResultCase.ERROR.equals(response.getResultCase())) {
                    throw new IOException(response.getError().getDetail());
                }
                sessionLinkImpl.remove(this);
                sessionLinkImpl = null;
            } catch (TimeoutException e) {
                LOG.warn("closing resource is timeout", e);
            }
        }
        handle = null;
    }
}
