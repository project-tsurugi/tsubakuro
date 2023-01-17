package com.tsurugidb.tsubakuro.datastore.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.tsurugidb.datastore.proto.DatastoreRequest;
import com.tsurugidb.tsubakuro.datastore.Backup;
import com.tsurugidb.tsubakuro.datastore.BackupType;
import com.tsurugidb.tsubakuro.datastore.BackupDetail;
import com.tsurugidb.tsubakuro.datastore.BackupEstimate;
import com.tsurugidb.tsubakuro.datastore.DatastoreClient;
import com.tsurugidb.tsubakuro.datastore.DatastoreService;
import com.tsurugidb.tsubakuro.datastore.DatastoreServiceCode;
import com.tsurugidb.tsubakuro.datastore.DatastoreServiceException;
import com.tsurugidb.tsubakuro.datastore.Tag;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class DatastoreClientImplTest {

    @Test
    void beginBackup() throws Exception {
        List<Path> files = List.of(Path.of("a"), Path.of("b"), Path.of("c"));
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<Backup> send(DatastoreRequest.BackupBegin request) throws IOException {
                assertEquals("LABEL", request.getLabel());
                return FutureResponse.returns(new BackupImpl(files));
            }
        });
        try (var backup = client.beginBackup("LABEL").await()) {
            assertEquals(files, backup.getFiles());
        }
    }

    @Test
    void beginDifferentialBackup() throws Exception {
        var confiturationId = "backup id";
        var logBegin = 123;
        var logEnd = 456;
        var imageFinish = Long.valueOf(789);
        List<BackupDetail.Entry> files = List.of(
            new BackupDetailImpl.Entry(Path.of("source_file1"), Path.of("destination_file1"), false, false),
            new BackupDetailImpl.Entry(Path.of("source_file2"), Path.of("destination_file2"), false, true),
            new BackupDetailImpl.Entry(Path.of("source_file3"), Path.of("destination_file3"), true, false),
            new BackupDetailImpl.Entry(Path.of("source_file4"), Path.of("destination_file4"), true, true)
        );
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<BackupDetail> send(DatastoreRequest.DifferentialBackupBegin request) throws IOException {
                assertEquals("LABEL", request.getLabel());
                assertEquals(DatastoreRequest.BackupType.STANDARD, request.getType());
                return FutureResponse.returns(
                    new BackupDetailImpl(confiturationId, logBegin, logEnd, imageFinish, files)
                );
            }
        });
        try (var backupDetail = client.beginBackup(BackupType.STANDARD, "LABEL").await()) {
            assertEquals(confiturationId, backupDetail.getConfigurationId());
            assertEquals(logBegin, backupDetail.getLogStart());
            assertEquals(logEnd, backupDetail.getLogFinish());
            assertEquals(imageFinish.longValue(), backupDetail.getImageFinish().getAsLong());
            assertEquals(files, backupDetail.nextEntries());
        }
    }

    @Test
    void estimateBackup() throws Exception {
        var estimate = new BackupEstimate(100, 200);
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<BackupEstimate> send(DatastoreRequest.BackupEstimate request)
                    throws IOException {
                return FutureResponse.returns(estimate);
            }
        });
        assertEquals(estimate, client.estimateBackup().await());
    }

    @Test
    void listTag() throws Exception {
        var tags = List.of(
                new Tag("1", "2", "3", Instant.ofEpochMilli(4)),
                new Tag("5", "6", "7", Instant.ofEpochMilli(8)),
                new Tag("9", null, null, Instant.ofEpochMilli(10)));
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<List<Tag>> send(DatastoreRequest.TagList request) throws IOException {
                return FutureResponse.returns(tags);
            }
        });
        assertEquals(tags, client.listTag().await());
    }

    @Test
    void getTag() throws Exception {
        Tag tag = new Tag("1", "2", "3", Instant.ofEpochMilli(4));
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<Optional<Tag>> send(DatastoreRequest.TagGet request) throws IOException {
                if (request.getName().equals("existing")) {
                    return FutureResponse.returns(Optional.of(tag));
                }
                if (request.getName().equals("missing")) {
                    return FutureResponse.returns(Optional.empty());
                }
                return fail();
            }
        });
        assertEquals(Optional.of(tag), client.getTag("existing").await());
        assertEquals(Optional.empty(), client.getTag("missing").await());
    }

    @Test
    void addTag() throws Exception {
        Tag tag = new Tag("1", "2", "3", Instant.ofEpochMilli(4));
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<Tag> send(DatastoreRequest.TagAdd request) throws IOException {
                if (request.getName().equals("existing")) {
                    return FutureResponse.raises(new DatastoreServiceException(DatastoreServiceCode.TAG_ALREADY_EXISTS));
                }
                if (request.getName().equals("missing")) {
                    return FutureResponse.returns(tag);
                }
                return fail();
            }
        });
        assertThrows(DatastoreServiceException.class, () -> client.addTag("existing", null).await());
        assertEquals(tag, client.addTag("missing", "comments").await());
    }

    @Test
    void removeTag() throws Exception {
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<Boolean> send(DatastoreRequest.TagRemove request) throws IOException {
                if (request.getName().equals("existing")) {
                    return FutureResponse.returns(Boolean.TRUE);
                }
                if (request.getName().equals("missing")) {
                    return FutureResponse.returns(Boolean.FALSE);
                }
                return fail();
            }
        });
        assertTrue(client.removeTag("existing").await());
        assertFalse(client.removeTag("missing").await());
    }
}
