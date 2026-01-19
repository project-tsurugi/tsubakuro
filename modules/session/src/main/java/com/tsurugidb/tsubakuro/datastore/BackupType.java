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
package com.tsurugidb.tsubakuro.datastore;

import com.tsurugidb.datastore.proto.DatastoreRequest;

/**
 * Backup type for datastore operations.
 */
public enum BackupType {
    /**
     * Standard backup type.
     */
    STANDARD(DatastoreRequest.BackupType.STANDARD),

    /**
     * Transaction backup type.
     */
    TRANSACTION(DatastoreRequest.BackupType.TRANSACTION);

    private final DatastoreRequest.BackupType type;

    BackupType(DatastoreRequest.BackupType type) {
        this.type = type;
    }

    /**
     * Gets the corresponding DatastoreRequest.BackupType.
     * @return the DatastoreRequest.BackupType
     */
    public DatastoreRequest.BackupType type() {
        return type;
    }
}
