package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionLinkImpl;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.session.ProtosForTest;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Lang;
import com.nautilus_technologies.tsubakuro.util.Pair;

class SessionLinkImplTest {
    ResponseProtos.Response nextResponse;

    class FutureResponseMock<V> implements FutureResponse<V> {
        private final SessionWireMock wire;
        private final Distiller<V> distiller;
        private ResponseWireHandle handle; // dummy
        FutureResponseMock(SessionWireMock wire, Distiller<V> distiller) {
            this.wire = wire;
            this.distiller = distiller;
        }
        @Override
        public V get() throws IOException {
            var response = wire.receive(handle);
            if (Objects.isNull(response)) {
                throw new IOException("received null response at FutureResponseMock, probably test program is incomplete");
            }
            return distiller.distill(response);
        }
        @Override
        public V get(long timeout, TimeUnit unit) throws IOException {
            return get();  // FIXME need to be implemented properly, same as below
        }
        @Override
        public boolean isDone() {
            return true;
        }
        @Override
        public void close() throws IOException, ServerException, InterruptedException {
            Lang.pass();
        }
    }

    class SessionWireMock implements SessionWire {
        @Override
        public <V> FutureResponse<V> send(long serviceID, RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
            switch (request.getRequestCase()) {
            case BEGIN:
                nextResponse = ProtosForTest.BeginResponseChecker.builder().build();
                return new FutureResponseMock<V>(this, distiller);
            case PREPARE:
                nextResponse = ProtosForTest.PrepareResponseChecker.builder().build();
                return new FutureResponseMock<V>(this, distiller);
            case DISPOSE_PREPARED_STATEMENT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<V>(this, distiller);
            case DISCONNECT:
                nextResponse = ProtosForTest.ResultOnlyResponseChecker.builder().build();
                return new FutureResponseMock<V>(this, distiller);
            case EXPLAIN:
                nextResponse = ProtosForTest.ExplainResponseChecker.builder().build();
                return new FutureResponseMock<V>(this, distiller);
            default:
                return null;  // dummy as it is test for session
            }
        }

        @Override
        public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> sendQuery(long serviceID, RequestProtos.Request.Builder request) throws IOException {
            return null;  // dummy as it is test for session
        }

        @Override
        public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
            var r = nextResponse;
            nextResponse = null;
            return r;
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null;  // dummy as it is test for session
        }

        @Override
        public ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            var r = nextResponse;
            nextResponse = null;
            return r;
        }

        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
        }

        @Override
        public void close() throws IOException {
        }
    }

    @Test
    void explain() throws Exception {
        SessionLinkImpl sessionLink = new SessionLinkImpl(new SessionWireMock());
        var r = sessionLink.send(ProtosForTest.ExplainChecker.builder());
        assertEquals(ProtosForTest.ResMessageExplainChecker.builder().build().getOutput(), r.get());
    }
}
