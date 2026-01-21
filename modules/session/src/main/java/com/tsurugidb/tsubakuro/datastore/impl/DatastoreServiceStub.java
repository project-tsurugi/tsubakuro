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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.tsurugidb.datastore.proto.DatastoreCommon;
import com.tsurugidb.datastore.proto.DatastoreRequest;
import com.tsurugidb.datastore.proto.DatastoreResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.MainResponseProcessor;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.datastore.Backup;
import com.tsurugidb.tsubakuro.datastore.BackupDetail;
// import com.tsurugidb.tsubakuro.datastore.BackupEstimate;
import com.tsurugidb.tsubakuro.datastore.DatastoreClient;
import com.tsurugidb.tsubakuro.datastore.DatastoreService;
import com.tsurugidb.tsubakuro.datastore.DatastoreServiceCode;
import com.tsurugidb.tsubakuro.datastore.DatastoreServiceException;
import com.tsurugidb.tsubakuro.datastore.Tag;
import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

/**
 * An implementation of {@link DatastoreService} communicate to the datastore service.
 */
public class DatastoreServiceStub implements DatastoreService {

    static final Logger LOG = LoggerFactory.getLogger(DatastoreServiceStub.class);

    /**
     * The datastore service ID.
     */
    public static final int SERVICE_ID = Constants.SERVICE_ID_BACKUP;

    private final Session session;

    private final ServerResourceHolder resources = new ServerResourceHolder();

    /**
     * Creates a new instance.
     * @param session the current session
     */
    public DatastoreServiceStub(@Nonnull Session session) {
        Objects.requireNonNull(session);
        this.session = session;
    }

    static DatastoreServiceException newUnknown(@Nonnull DatastoreResponse.UnknownError message) {
        assert message != null;
        return new DatastoreServiceException(DatastoreServiceCode.UNKNOWN, message.getMessage());
    }

    static BrokenResponseException newResultNotSet(
            @Nonnull Class<? extends Message> aClass, @Nonnull String name) {
        assert aClass != null;
        assert name != null;
        return new BrokenResponseException(MessageFormat.format(
                "{0}.{1} is not set",
                aClass.getSimpleName(),
                name));
    }

    static Tag convert(@Nonnull DatastoreCommon.Tag tag) {
        assert tag != null;
        return new Tag(
                tag.getName(),
                optional(tag.getComment()),
                optional(tag.getAuthor()),
                Instant.ofEpochMilli(tag.getTimestamp()));
    }

    private static @Nullable String optional(@Nullable String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return value;
    }

    private static DatastoreRequest.Request.Builder newRequest() {
        return DatastoreRequest.Request.newBuilder()
                .setServiceMessageVersionMajor(DatastoreClient.SERVICE_MESSAGE_VERSION_MAJOR)
                .setServiceMessageVersionMinor(DatastoreClient.SERVICE_MESSAGE_VERSION_MINOR);
    }

    class BackupBeginProcessor implements MainResponseProcessor<Backup> {
        @Override
        public Backup process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DatastoreResponse.BackupBegin.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var backupId = message.getSuccess().getId();
                var files = new ArrayList<Path>();
                for (var f : message.getSuccess().getSimpleSource().getFilesList()) {
                    files.add(Path.of(f));
                }
                return resources.register(new BackupImpl(DatastoreServiceStub.this, resources, backupId, files));

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Backup> send(@Nonnull DatastoreRequest.BackupBegin request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
            SERVICE_ID,
            toDelimitedByteArray(newRequest()
                                 .setBackupBegin(request)
                                 .build()),
            new BackupBeginProcessor().asResponseProcessor());
    }

    class BackupDetailBeginProcessor implements MainResponseProcessor<BackupDetail> {
        @Override
        public BackupDetail process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DatastoreResponse.BackupBegin.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var successMessage = message.getSuccess();
                var backupId = successMessage.getId();
                if (successMessage.getSourceCase() == DatastoreResponse.BackupBegin.Success.SourceCase.DETAIL_SOURCE) {
                    var detailSourceMessage = successMessage.getDetailSource();
                    var files = new ArrayList<BackupDetail.Entry>();
                    for (var f : detailSourceMessage.getDetailFilesList()) {
                        files.add(new BackupDetailImpl.Entry(
                            Path.of(f.getSource()),
                            Path.of(f.getDestination()),
                            f.getMutable(),
                            f.getDetached()
                        ));
                    }
                    return resources.register(new BackupDetailImpl(
                        DatastoreServiceStub.this,
                        resources,
                        Long.valueOf(backupId).toString(),
                        detailSourceMessage.getLogBegin(),
                        detailSourceMessage.getLogEnd(),
                        detailSourceMessage.getImageFinishCase() == DatastoreResponse.BackupBegin.DetailSource.ImageFinishCase.IMAGE_FINISH_VALUE
                            ? detailSourceMessage.getImageFinishValue() : null,
                        files));
                }
                break;

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<BackupDetail> send(@Nonnull DatastoreRequest.BackupDetailBegin request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
            SERVICE_ID,
            toDelimitedByteArray(newRequest()
                                 .setBackupDetailBegin(request)
                                 .build()),
            new BackupDetailBeginProcessor().asResponseProcessor());
    }

    static class BackupEndProcessor implements MainResponseProcessor<Void> {
        @Override
        public Void process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DatastoreResponse.BackupEnd.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return null;

            case EXPIRED:
                throw new DatastoreServiceException(DatastoreServiceCode.BACKUP_EXPIRED);

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Void> send(@Nonnull DatastoreRequest.BackupEnd request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
            SERVICE_ID,
            toDelimitedByteArray(newRequest()
                                 .setBackupEnd(request)
                                 .build()),
            new BackupEndProcessor().asResponseProcessor());
    }

    @Override
    public FutureResponse<Void> updateExpirationTime(long time, @Nonnull TimeUnit unit) throws IOException {
        return session.updateExpirationTime(time, unit);
    }

    static class TagListProcessor implements MainResponseProcessor<List<Tag>> {
        @Override
        public List<Tag> process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DatastoreResponse.TagList.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                var list = new ArrayList<Tag>(message.getSuccess().getTagsCount());
                for (var tag : message.getSuccess().getTagsList()) {
                    list.add(convert(tag));
                }
                return Collections.unmodifiableList(list);

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<List<Tag>> send(@Nonnull DatastoreRequest.TagList request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(newRequest()
                                     .setTagList(request)
                                     .build()),
                new TagListProcessor().asResponseProcessor());
    }

    static class TagAddProcessor implements MainResponseProcessor<Tag> {
        @Override
        public Tag process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DatastoreResponse.TagAdd.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return convert(message.getSuccess().getTag());

            case ALREADY_EXISTS:
                throw new DatastoreServiceException(
                        DatastoreServiceCode.TAG_ALREADY_EXISTS,
                        MessageFormat.format(
                                "tag is already exists: '{0}'",
                                message.getAlreadyExists().getName()));

            case TOO_LONG_NAME:
                throw new DatastoreServiceException(
                        DatastoreServiceCode.TAG_NAME_TOO_LONG,
                        MessageFormat.format(
                                "tag name length is exceeded (max {1} characters): '{0}'",
                                message.getTooLongName().getName(),
                                message.getTooLongName().getMaxCharacters()));

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Tag> send(@Nonnull DatastoreRequest.TagAdd request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(newRequest()
                                     .setTagAdd(request)
                                     .build()),
                new TagAddProcessor().asResponseProcessor());
    }

    static class TagGetProcessor implements MainResponseProcessor<Optional<Tag>> {
        @Override
        public Optional<Tag> process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DatastoreResponse.TagGet.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return Optional.of(convert(message.getSuccess().getTag()));

            case NOT_FOUND:
                return Optional.empty();

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Optional<Tag>> send(@Nonnull DatastoreRequest.TagGet request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(newRequest()
                                     .setTagGet(request)
                                     .build()),
                new TagGetProcessor().asResponseProcessor());
    }

    static class TagRemoveProcessor implements MainResponseProcessor<Boolean> {
        @Override
        public Boolean process(ByteBuffer payload) throws IOException, ServerException, InterruptedException {
            var message = DatastoreResponse.TagRemove.parseDelimitedFrom(new ByteBufferInputStream(payload));
            LOG.trace("receive: {}", message); //$NON-NLS-1$
            switch (message.getResultCase()) {
            case SUCCESS:
                return Boolean.TRUE;

            case NOT_FOUND:
                return Boolean.FALSE;

            case UNKNOWN_ERROR:
                throw newUnknown(message.getUnknownError());

            case RESULT_NOT_SET:
                throw newResultNotSet(message.getClass(), "result"); //$NON-NLS-1$

            default:
                break;
            }
            throw new AssertionError(); // may not occur
        }
    }

    @Override
    public FutureResponse<Boolean> send(@Nonnull DatastoreRequest.TagRemove request) throws IOException {
        LOG.trace("send: {}", request); //$NON-NLS-1$
        return session.send(
                SERVICE_ID,
                toDelimitedByteArray(newRequest()
                                     .setTagRemove(request)
                                     .build()),
                new TagRemoveProcessor().asResponseProcessor());
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        LOG.trace("closing underlying resources"); //$NON-NLS-1$
        resources.close();
    }

// FIXME should process at transport layer
    private byte[] toDelimitedByteArray(DatastoreRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }
}
