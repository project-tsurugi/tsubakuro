package com.nautilus_technologies.tsubakuro.impl.low.backup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.sql.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.backup.DatastoreClient;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;

class SessionImplTest {
    class SessionWireMock implements SessionWire {
        @Override
        public <V> FutureResponse<V> send(long serviceID, RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public Pair<FutureResponse<ResponseProtos.ExecuteQuery>, FutureResponse<ResponseProtos.ResultOnly>> sendQuery(
                long serviceID, RequestProtos.Request.Builder request) throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public ResponseProtos.Response receive(ResponseWireHandle handle) throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public ResponseProtos.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            return null; // dummy as it is test for session
        }

        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
        }

        @Override
        public void close() throws IOException {
        }
    }

    @Test
    void getPath() {
        try (var session = new SessionImpl()) {
            session.connect(new SessionWireMock());
            try (
                var client = DatastoreClient.attach(session);
                var backup = client.beginBackup().await();
            ) {
                for (Path source : backup.files()) {
                    assertEquals(Path.of("/tmp/backup-1"), source);
                }
            } catch (Exception e) {
                System.err.println(e);
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
