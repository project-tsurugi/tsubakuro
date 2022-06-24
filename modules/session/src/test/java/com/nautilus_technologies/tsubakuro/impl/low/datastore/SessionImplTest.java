package com.nautilus_technologies.tsubakuro.impl.low.datastore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Wire;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.ResponseWireHandle;
import com.nautilus_technologies.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.common.connection.wire.Response;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.nautilus_technologies.tsubakuro.util.Owner;
import com.nautilus_technologies.tsubakuro.impl.low.common.SessionImpl;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreClient;
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
        public ResponseWireHandle responseWireHandle() {
            return null;
        }

        @Override
        public void release() {
        }

        @Override
        public void setQueryMode() {
        }

        @Override
        public void close() throws IOException, InterruptedException {
        }
    }

    class SessionWireMock implements Wire {
        @Override
        public ResultSetWire createResultSetWire() throws IOException {
            return null; // dummy as it is test for session
        }
    
        @Override
        public void release(ResponseWireHandle responseWireHandle) {
        }

        @Override
        public void setQueryMode(ResponseWireHandle responseWireHandle) {
        }

        @Override
        public FutureResponse<? extends Response> send(int serviceID, byte[] request) {
            var response = new TestResponse(this);
            return FutureResponse.wrap(Owner.of(response));
        }

        @Override
        public FutureResponse<? extends Response> send(int serviceID, ByteBuffer request) {
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
