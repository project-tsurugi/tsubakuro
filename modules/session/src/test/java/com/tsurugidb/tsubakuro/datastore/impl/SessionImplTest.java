/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

        TestResponse(SessionWireMock wire) {
            this.wire = wire;
        }

        @Override
        public boolean isMainResponseReady() {
            return true;
        }
    
        @Override
        public ByteBuffer waitForMainResponse() throws IOException {
            if (isMainResponseReady()) {
                try (var buffer = new ByteArrayOutputStream()) {
                    var response = DatastoreResponse.BackupBegin.newBuilder()
                        .setSuccess(DatastoreResponse.BackupBegin.Success.newBuilder()
                            .setId(100)
                            .setSimpleSource(DatastoreResponse.BackupBegin.SimpleSource.newBuilder()
                                .addFiles("/tmp/backup-1").build())
                        .build())
                    .build();
                    response.writeDelimitedTo(buffer);
                    return ByteBuffer.wrap(buffer.toByteArray());
                } catch (IOException e) {
                    System.out.println(e);
                }
                return null; // dummy as it is test for session
            }
            throw new IOException("response box is not available");
        }
    
        @Override
        public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException, TimeoutException {
            if (isMainResponseReady()) {
                return waitForMainResponse();
            }
            throw new IOException("response box is not available");  // FIXME arch. mismatch??
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
                System.err.println(e);
                e.printStackTrace();
                System.err.println(e);
            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
            System.err.println(e);
        }
    }
}
