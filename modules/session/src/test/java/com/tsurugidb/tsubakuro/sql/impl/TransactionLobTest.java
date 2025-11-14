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
package com.tsurugidb.tsubakuro.sql.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.sql.BlobReference;
import com.tsurugidb.tsubakuro.sql.ClobReference;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.impl.TransactionLobTest.BlobReferenceForTest;
import com.tsurugidb.tsubakuro.sql.io.BlobException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

class TransactionLobTest {

    private final String CHANNEL_NAME_FOR_TEST = "ChannelNameForTest";
    private final int RESPONSE_MESSAGE_SIZE = 26;

    SqlResponse.Response nextResponse;

    class ChannelResponseMock implements Response {
        @Override
        public boolean isMainResponseReady() {
            return true;
        }
        @Override
        public ByteBuffer waitForMainResponse() throws IOException {
            return ByteBuffer.wrap(DelimitedConverter.toByteArray(nextResponse));
        }
        @Override
        public ByteBuffer waitForMainResponse(long timeout, TimeUnit unit) throws IOException {
            return waitForMainResponse();
        }
        @Override
        public InputStream openSubResponse(String id) throws IOException {
            if (id.equals(CHANNEL_NAME_FOR_TEST)) {
                byte[] response = new byte[RESPONSE_MESSAGE_SIZE];
                for (int i = 0; i < RESPONSE_MESSAGE_SIZE; i++) {
                    response[i] = (byte) ('a' + i);
                }
                return new ByteArrayInputStream(response);
            }
            throw new IOException("illegal channel name: " + id);
        }
        public InputStream openSubResponse(String id, long timeout, TimeUnit unit) throws IOException {
            return openSubResponse(id);
        }
        @Override
        public void close() throws IOException, InterruptedException {
        }
    }

    class SessionWireMock implements Wire {
        private boolean closed = false;

        @Override
        public FutureResponse<? extends Response> send(int serviceID, byte[] byteArray) throws IOException {
            if (closed) {
                throw new IOException("link is already closed");
            }

            var request = SqlRequest.Request.parseDelimitedFrom(new ByteArrayInputStream(byteArray));
            switch (request.getRequestCase()) {
                case BEGIN:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setBegin(SqlResponse.Begin.newBuilder()
                                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                                        .setTransactionHandle(SqlCommon.Transaction.newBuilder().setHandle(100).build()))
                                        .build())
                        .build();
                    break;
                case PREPARE:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setPrepare(SqlResponse.Prepare.newBuilder()
                                        .setPreparedStatementHandle(SqlCommon.PreparedStatement.newBuilder().setHandle(12345).build())
                                        .build())
                        .build();
                    break;
                case GET_LARGE_OBJECT_DATA:
                    nextResponse = SqlResponse.Response.newBuilder()
                        .setGetLargeObjectData(SqlResponse.GetLargeObjectData.newBuilder()
                                       .setSuccess(SqlResponse.GetLargeObjectData.Success.newBuilder().setChannelName(CHANNEL_NAME_FOR_TEST)))
                        .build();
                    break;
                default:
                    System.out.println("falls default case%n" + request);
                    return null;  // dummy as it is test for session
            }
            return FutureResponse.wrap(Owner.of(new ChannelResponseMock()));
        }

        @Override
        public FutureResponse<? extends Response> send(int serviceID, ByteBuffer request) throws IOException {
            return send(serviceID, request.array());
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    @Test
    void openInputStream_success() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        var transaction = sqlClient.createTransaction().await();

        var blobReference = new BlobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), 12345, 678);
        var stream = transaction.openInputStream(blobReference).await();
        for (int i = 0; i < RESPONSE_MESSAGE_SIZE; i++) {
            assertEquals('a' + i, stream.read());
        }
    }

    class BlobReferenceForTest implements BlobReference {
    }

    @Test
    void openInputStream_exception() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        var transaction = sqlClient.createTransaction().await();

        Throwable exception = assertThrows(IllegalStateException.class, () -> {
            transaction.openInputStream(new BlobReferenceForTest());
        });
    }

    @Test
    void openReader_success() throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        var transaction = sqlClient.createTransaction().await();

        var clobReference = new ClobReferenceForSql(SqlCommon.LargeObjectProvider.forNumber(2), 12345, 678);
        var reader = transaction.openReader(clobReference).await();
        for (int i = 0; i < RESPONSE_MESSAGE_SIZE; i++) {
            assertEquals('a' + i, reader.read());
        }
    }

    class ClobReferenceForTest implements ClobReference {
    }

    @Test
    void blobParameter_exception(@TempDir Path tempDir) throws Exception {
        var session = new SessionImpl();
        session.connect(new SessionWireMock());
        var sqlClient = SqlClient.attach(session);
        var transaction = sqlClient.createTransaction().await();
        var preparedStatement = sqlClient.prepare("select * from table1").await();

        assertThrows(BlobException.class, () ->
            transaction.executeStatement(preparedStatement, Parameters.blobOf("blob1", tempDir.resolve("blob.data"))));
    }
}
