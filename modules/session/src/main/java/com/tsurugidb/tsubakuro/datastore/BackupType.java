package com.tsurugidb.tsubakuro.datastore;

import com.tsurugidb.datastore.proto.DatastoreRequest;

public enum BackupType {
    STANDARD(DatastoreRequest.BackupType.STANDARD),
    TRANSACTION(DatastoreRequest.BackupType.TRANSACTION);

    private final DatastoreRequest.BackupType type;

    BackupType(DatastoreRequest.BackupType type) {
        this.type = type;
    }

    public DatastoreRequest.BackupType type() {
        return type;
    }
}
