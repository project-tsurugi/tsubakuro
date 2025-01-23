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

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.sql.ClobReference;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;

/**
 * Represents a reference to CLOB data and corresponding Response.
 */
public final class ClobReferenceForSql implements ClobReference {
    final SqlCommon.LargeObjectReference largeObjectReference;
    Response response;

    public ClobReferenceForSql(long objectId) {
        largeObjectReference = SqlCommon.LargeObjectReference.newBuilder()
        .setProvider(SqlCommon.LargeObjectProvider.SQL)
        .setObjectId(objectId)
        .build();
        this.response = null;
    }

    public ClobReferenceForSql setResponse(Response res) {
        response = res;
        return this;
    }

    SqlCommon.LargeObjectReference clobReference() {
        return largeObjectReference;
    }

    Response response() {
        return response;
    }
}
