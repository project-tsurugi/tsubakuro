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
package com.tsurugidb.tsubakuro.kvs.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.tsurugidb.kvs.proto.KvsRequest;
import com.tsurugidb.kvs.proto.KvsResponse;
import com.tsurugidb.kvs.proto.KvsRequest.Request;
import com.tsurugidb.kvs.proto.KvsTransaction.Handle;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.mock.RequestHandler;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class StubUtils {

    static RequestHandler accepts(KvsRequest.Request.CommandCase command, RequestHandler next) {
        return new RequestHandler() {
            @Override
            public Response handle(int serviceId, ByteBuffer request) throws IOException, ServerException {
                Request req = KvsRequest.Request.parseDelimitedFrom(new ByteBufferInputStream(request));
                assertEquals(command, req.getCommandCase());
                return next.handle(serviceId, request);
            }
        };
    }

    static KvsResponse.Void newVoid() {
        return KvsResponse.Void.newBuilder()
                .build();
    }

    static KvsResponse.Error newError(int code, String req) {
        return KvsResponse.Error.newBuilder()
                .setCode(code)
                .setDetail(req)
                .build();
    }

    static final KvsServiceCode ERR_CODE = KvsServiceCode.INVALID_ARGUMENT;
    static final String ERR_DETAIL = "error occured";

    static KvsResponse.Error newError() {
        return newError(ERR_CODE.getCodeNumber(), ERR_DETAIL);
    }

    static void checkException(KvsServiceException e) {
        assertEquals(ERR_CODE, e.getDiagnosticCode());
        assertTrue(e.getMessage().contains(ERR_CODE.getStructuredCode()));
        assertTrue(e.getMessage().contains(ERR_DETAIL));
    }

    static KvsResponse.Response newBegin(long systemId) {
        return KvsResponse.Response.newBuilder().setBegin(
                KvsResponse.Begin.newBuilder()
                    .setSuccess(KvsResponse.Begin.Success.newBuilder()
                        .setTransactionHandle(
                                Handle.newBuilder().setSystemId(systemId).build())
                        .build())
                    .build())
                .build();
    }

    static KvsResponse.Response newCommit() {
        return KvsResponse.Response.newBuilder().setCommit(
                KvsResponse.Commit.newBuilder()
                .setSuccess(newVoid()).build()).build();
    }

    static KvsResponse.Response newRollback() {
        return KvsResponse.Response.newBuilder().setRollback(
                KvsResponse.Rollback.newBuilder()
                .setSuccess(newVoid()).build()).build();
    }

    static KvsResponse.Response newDispose() {
        return KvsResponse.Response.newBuilder().setDisposeTransaction(
                KvsResponse.DisposeTransaction.newBuilder()
                .setSuccess(newVoid()).build()).build();
    }

    static RequestHandler newAcceptBegin(long systemId) throws IOException {
        return accepts(KvsRequest.Request.CommandCase.BEGIN,
                RequestHandler.returns(newBegin(systemId)));
    }

    static RequestHandler newAcceptCommit() throws IOException {
        return accepts(KvsRequest.Request.CommandCase.COMMIT,
                RequestHandler.returns(newCommit()));
    }

    static RequestHandler newAcceptRollback() throws IOException {
        return accepts(KvsRequest.Request.CommandCase.ROLLBACK,
                RequestHandler.returns(newRollback()));
    }

    static RequestHandler newAcceptDispose() throws IOException {
        return accepts(KvsRequest.Request.CommandCase.DISPOSE_TRANSACTION,
                RequestHandler.returns(newDispose()));
    }

}
