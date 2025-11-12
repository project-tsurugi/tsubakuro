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

/**
 * Represents a reference to CLOB data and corresponding Response.
 */
public final class ClobReferenceForSql implements ClobReference {
    final SqlCommon.LargeObjectReference largeObjectReference;

    public ClobReferenceForSql(SqlCommon.LargeObjectProvider provider, long objectId, long referenceTag) {
        largeObjectReference = SqlCommon.LargeObjectReference.newBuilder()
                                .setProvider(provider)
                                .setObjectId(objectId)
                                .setReferenceTag(referenceTag)
                                .build();
    }

    SqlCommon.LargeObjectReference clobReference() {
        return largeObjectReference;
    }

    // for tests
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClobReferenceForSql) {
            ClobReferenceForSql other = (ClobReferenceForSql) obj;
            return largeObjectReference.equals(other.largeObjectReference);
        }
        return false;
    }
    @Override
    public int hashCode() {
        return largeObjectReference.hashCode();
    }
    public long getProvider() {
        return largeObjectReference.getProvider().getNumber();
    }
    public long getObjectId() {
        return largeObjectReference.getObjectId();
    }
    public long getReferenceTag() {
        return largeObjectReference.getReferenceTag();
    }
}
