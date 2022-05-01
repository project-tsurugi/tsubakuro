package com.nautilus_technologies.tsubakuro.impl.low.backup;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.nio.file.Path;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tsubakuro.low.backup.Backup;

import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.channel.common.sql.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;

import org.junit.jupiter.api.Test;

class SessionImplTest {
    class SessionWireMock implements SessionWire {
        @Override
        public <V> Future<V> send(RequestProtos.Request.Builder request, Distiller<V> distiller) throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public Pair<Future<ResponseProtos.ExecuteQuery>, Future<ResponseProtos.ResultOnly>> sendQuery(
                RequestProtos.Request.Builder request) throws IOException {
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
        SessionImpl session;
        try {
            session = new SessionImpl();
            session.connect(new SessionWireMock());

            Future<Backup> fBackup = session.beginBackup();
            try (Backup backup = fBackup.get()) {
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
