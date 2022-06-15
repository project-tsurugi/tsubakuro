package com.nautilus_technologies.tsubakuro.impl.low.datastore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
// import java.io.ByteArrayByteBuffer;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.SessionWire;
import com.nautilus_technologies.tsubakuro.channel.common.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.wire.Response;
import com.nautilus_technologies.tsubakuro.exception.ServerException;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Owner;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreClient;
import com.nautilus_technologies.tsubakuro.protos.Distiller;
import com.tsurugidb.jogasaki.proto.SqlRequest;
import com.tsurugidb.jogasaki.proto.SqlResponse;
import com.nautilus_technologies.tsubakuro.util.Pair;
import com.nautilus_technologies.tateyama.proto.DatastoreResponseProtos;

class SessionImplTest {
    public class TestResponse implements Response {
        private final SessionWireMock wire;
        private ResponseWireHandle handle;

        TestResponse(SessionWireMock wire) {
            this.wire = wire;
            this.handle = null;
        }

        @Override
        public boolean isMainResponseReady() {
            return true;
        }
    
        @Override
        public ByteBuffer waitForMainResponse() throws IOException {
            if (isMainResponseReady()) {
                return wire.response(handle);
            }
            throw new IOException("response box is not available");
        }
    
        @Override
        public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
            if (isMainResponseReady()) {
                return wire.response(handle, timeout, unit);
            }
            throw new IOException("response box is not available");  // FIXME arch. mismatch??
        }
    
        @Override
        public Collection<String> getSubResponseIds() throws IOException, ServerException, InterruptedException {
            return null;
        }
    
        @Override
        public InputStream openSubResponse(String id) throws IOException, ServerException, InterruptedException {
            return null;
        }

        @Override
        public void close() throws IOException, InterruptedException {
        }
    }

    class SessionWireMock implements SessionWire {
        @Override
        public <V> FutureResponse<V> send(long serviceID, SqlRequest.Request.Builder request, Distiller<V> distiller) throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public Pair<FutureResponse<SqlResponse.ExecuteQuery>, FutureResponse<SqlResponse.ResultOnly>> sendQuery(
                long serviceID, SqlRequest.Request.Builder request) throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle) throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null; // dummy as it is test for session
        }

        @Override
        public SqlResponse.Response receive(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            return null; // dummy as it is test for session
        }

        @Override
        public void unReceive(ResponseWireHandle responseWireHandle) {
        }

        @Override
        public FutureResponse<? extends Response> send(long serviceID, byte[] request) {
            var response = new TestResponse(this);
            return FutureResponse.wrap(Owner.of(response));
        }

        @Override
        public FutureResponse<? extends Response> send(long serviceID, ByteBuffer request) {
            var response = new TestResponse(this);
            return FutureResponse.wrap(Owner.of(response));
        }

        @Override
        public ByteBuffer response(ResponseWireHandle handle) {
            try (var buffer = new ByteArrayOutputStream()) {
                var response = DatastoreResponseProtos.BackupBegin.newBuilder()
                    .setSuccess(DatastoreResponseProtos.BackupBegin.Success.newBuilder()
                    .setId(100)
                    .addFiles("/tmp/backup-1")
                    .build())
                .build();
                response.writeDelimitedTo(buffer);
                return ByteBuffer.wrap(buffer.toByteArray());
            } catch (IOException e) {
                System.out.println(e);
            }
            return null; // dummy as it is test for session
        }

        public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) {
            return response(handle);
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
                for (Path source : backup.getFiles()) {
                    assertEquals(Path.of("/tmp/backup-1"), source);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e);
        }
    }
}
