/*
 * Copyright 2023-2025 Project Tsurugi.
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.OptionalLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.WireImpl;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.impl.SessionImpl;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;

class AsyncCloseTest {

    private final MockLink link = new MockLink();

    private WireImpl wire = null;

    private Session session = null;

    AsyncCloseTest() {
        try {
            wire = new WireImpl(link);
            session = new SessionImpl();
            session.connect(wire);
     } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException, ServerException {
        if (session != null) {
            session.close();
        }
    }

    @Test
    void transactionCloseWithCommit_success_get() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // add delay for DisposeTransaction
        link.next(new ResponseTimeoutException());
        // add handler for DisposeTransaction
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            tx.commit().get();
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void transactionCloseWithCommit_success_timeout() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add delay for Commit
        link.next(new ResponseTimeoutException());
        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // add delay for DisposeTransaction
        link.next(new ResponseTimeoutException());
        // add handler for DisposeTransaction
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            tx.commit();
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void transactionCloseWithCommit_failure_get() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                .setError(SqlResponse.Error.newBuilder()))
                    .build());

        // add delay for DisposeTransaction
        link.next(new ResponseTimeoutException());
        // add handler for DisposeTransaction
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            assertThrows(SqlServiceException.class, () ->
                tx.commit().get()
            );
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void transactionCloseWithCommit_failure_timeout() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add delay for Commit
        link.next(new ResponseTimeoutException());
        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setError(SqlResponse.Error.newBuilder()))
                    .build());

        // add delay for DisposeTransaction
        link.next(new ResponseTimeoutException());
        // add handler for DisposeTransaction
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {

            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            tx.commit();
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void autoDisposeTransactionCloseWithCommit_success_get() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            tx.commit(SqlRequest.CommitOption.newBuilder().setAutoDispose(true).build()).get();
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void autoDisposeTransactionCloseWithCommit_success_timeout() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add delay for Commit
        link.next(new ResponseTimeoutException());
        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            tx.commit(SqlRequest.CommitOption.newBuilder().setAutoDispose(true).build());
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void autoDisposeTransactionCloseWithCommit_failure_get() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setError(SqlResponse.Error.newBuilder()))
                    .build());

        // add delay for DisposeTransaction
        link.next(new ResponseTimeoutException());
        // add handler for DisposeTransaction
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            assertThrows(SqlServiceException.class, () ->
                tx.commit(SqlRequest.CommitOption.newBuilder().setAutoDispose(true).build()).get()
            );
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void autoDisposeTransactionCloseWithCommit_failure_timeout() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add delay for Commit
        link.next(new ResponseTimeoutException());
        // add response for Commit
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setError(SqlResponse.Error.newBuilder()))
                    .build());

        // add delay for DisposeTransaction
        link.next(new ResponseTimeoutException());
        // add handler for DisposeTransaction
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
            tx.commit(SqlRequest.CommitOption.newBuilder().setAutoDispose(true).build());
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }

    @Test
    void transactionCloseWithoutCommit_timeout() throws Exception {
        // add response for Begin
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                        .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                        .setHandle(100))))
                    .build());

        // add delay for Rollback
        link.next(new ResponseTimeoutException());
        // add response for Rollback
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // add delay for DisposeTransaction
        link.next(new ResponseTimeoutException());
        // add handler for DisposeTransaction
        link.next(SqlResponse.Response.newBuilder()
                    .setResultOnly(SqlResponse.ResultOnly.newBuilder()
                                    .setSuccess(SqlResponse.Success.newBuilder()))
                    .build());

        // Begin test body
        var message = SqlRequest.Begin.newBuilder()
                .setOption(SqlRequest.TransactionOption.getDefaultInstance())
                .build();
        try (
            var service = new SqlServiceStub(session);
            var future = service.send(message);
            var tx = future.await();
        ) {
            assertEquals(OptionalLong.of(100), TransactionImpl.getId(tx));
        }

        session.close();
        // neccesarry for taking care of delayed disposal
        // need session.close() followed by SessionImpl.waitForCompletion()
        if (session instanceof SessionImpl) {
            ((SessionImpl) session).waitForCompletion();
        }
        assertFalse(link.hasRemaining());
    }
}
