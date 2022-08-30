package com.tsurugidb.tsubakuro.sql.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
//import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.protobuf.Message;
//import com.tsurugidb.tateyama.proto.SqlRequest;
//import com.tsurugidb.tateyama.proto.SqlResponse;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseProcessor;
//import com.tsurugidb.tsubakuro.exception.BrokenResponseException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
//import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;
//import com.tsurugidb.tsubakuro.sql.io.StreamBackedValueInput;
//import com.tsurugidb.tsubakuro.util.Owner;
import com.tsurugidb.tsubakuro.util.ServerResourceHolder;

/**
 * Abstract implementation of {@link ResponseProcessor} for providing {@link ResultSet}.
 * @param <T> the response message type
 */
abstract class AbstractResultSetProcessor<T extends Message>
        implements ResponseProcessor<ResultSet>, ResultSetImpl.ResponseTester {

    private final ServerResourceHolder resources;

    //    private final String metadataChannel;

    //    private final String relationChannel;

    protected final AtomicReference<T> cache = new AtomicReference<>();

    private final AtomicBoolean passed = new AtomicBoolean();

    AbstractResultSetProcessor(@Nonnull ServerResourceHolder resources) {
        Objects.requireNonNull(resources);
        this.resources = resources;
    }

//    private void validateMetadata(
//            Response response) throws IOException, ServerException, InterruptedException {
//        if (!response.getSubResponseIds().contains(metadataChannel)) {
//            // if the response does not contain metadata, first we check main-response
//            test(response);
//            // or else, message seems to be broken
//            throw new BrokenResponseException(MessageFormat.format(
//                    "missing result set metadata in the response: ''{0}''",
//                    metadataChannel));
//        }
//    }

//    private void validateRelationData(
//            Response response) throws IOException, ServerException, InterruptedException {
//        if (!response.getSubResponseIds().contains(relationChannel)) {
//            // if the response does not contain relation data, first we check main-response
//            test(response);
//            // or else, message seems to be broken
//            throw new BrokenResponseException(MessageFormat.format(
//                    "missing result set relation data in the response: ''{0}''",
//                    relationChannel));
//        }
//    }

    @Override
    public void test(@Nonnull Response response) throws IOException, ServerException, InterruptedException {
        Objects.requireNonNull(response);
        if (passed.get()) {
            // already passed
            return;
        }
        if (cache.get() == null) {
            // FIXME consider timeout
            var message = parse(response.waitForMainResponse());
            cache.compareAndSet(null, message);
        }
        doTest(cache.get());
        passed.set(true);
    }

    abstract T parse(@Nonnull ByteBuffer payload) throws IOException;

    abstract void doTest(@Nonnull T response) throws IOException, ServerException, InterruptedException;
}
