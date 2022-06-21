package com.nautilus_technologies.tsubakuro.impl.low.datastore;

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

import com.nautilus_technologies.tateyama.proto.DatastoreRequestProtos;
import com.nautilus_technologies.tsubakuro.low.datastore.Backup;
import com.nautilus_technologies.tsubakuro.low.datastore.BackupEstimate;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreClient;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreService;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreServiceCode;
import com.nautilus_technologies.tsubakuro.low.datastore.DatastoreServiceException;
import com.nautilus_technologies.tsubakuro.low.datastore.Tag;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

class DatastoreClientImplTest {

    @Test
    void beginBackup() throws Exception {
        List<Path> files = List.of(Path.of("a"), Path.of("b"), Path.of("c"));
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<Backup> send(DatastoreRequestProtos.BackupBegin request) throws IOException {
                assertEquals("LABEL", request.getLabel());
                return FutureResponse.returns(new BackupImpl(files));
            }
        });
        try (var backup = client.beginBackup("LABEL").await()) {
            assertEquals(files, backup.getFiles());
        }
    }

    @Test
    void estimateBackup() throws Exception {
        var estimate = new BackupEstimate(100, 200);
        DatastoreClient client = new DatastoreClientImpl(new DatastoreService() {
            @Override
            public FutureResponse<BackupEstimate> send(DatastoreRequestProtos.BackupEstimate request)
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
            public FutureResponse<List<Tag>> send(DatastoreRequestProtos.TagList request) throws IOException {
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
            public FutureResponse<Optional<Tag>> send(DatastoreRequestProtos.TagGet request) throws IOException {
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
            public FutureResponse<Tag> send(DatastoreRequestProtos.TagAdd request) throws IOException {
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
            public FutureResponse<Boolean> send(DatastoreRequestProtos.TagRemove request) throws IOException {
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
