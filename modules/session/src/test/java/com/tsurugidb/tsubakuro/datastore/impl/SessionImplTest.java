package com.tsurugidb.tsubakuro.datastore.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseWireHandle;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.datastore.DatastoreClient;
import com.tsurugidb.datastore.proto.DatastoreResponse;

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
        public TestResponse duplicate() {
            return null;
        }

        @Override
        public void setResultSetMode() {
        }

        @Override
        public void release() {
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
        public void setResultSetMode(ResponseWireHandle responseWireHandle) {
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
                var response = DatastoreResponse.BackupBegin.newBuilder()
                    .setSuccess(DatastoreResponse.BackupBegin.Success.newBuilder()
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
