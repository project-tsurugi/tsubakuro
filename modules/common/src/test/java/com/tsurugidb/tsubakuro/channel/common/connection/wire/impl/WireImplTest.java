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
package com.tsurugidb.tsubakuro.channel.common.connection.wire.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Message;
import com.tsurugidb.framework.proto.FrameworkCommon;
import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;
import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.mock.MockLink;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

class WireImplTest {
    private static final int SERVICE_ID_FOR_THE_TEST = 999;

    private final MockLink link = new MockLink();
    private WireImpl wire = null;

    static class BlobInfoForTest implements BlobInfo {
        private final String channelName;
        private final Path path;

        public BlobInfoForTest(String channelName, Path path) {
            this.channelName = channelName;
            this.path = path;
        }

        @Override
        public String getChannelName() {
            return channelName;
        }

        @Override
        public boolean isFile() {
            return true;
        }

        @Override
        public Optional<Path> getPath() {
            return Optional.of(path);
        }
    }

    public WireImplTest() {
        try {
            wire = new WireImpl(link);
        } catch (IOException e) {
            System.err.println(e);
            fail("fail to create WireImpl");
        }
    }

    @Test
    void sendBlob() throws Exception {
        String fileName1 = "/tmp/channelFile-1";
        String channelName1 = "testChannelFile-1";
        String fileName2 = "/tmp/channel-2";
        String channelName2 = "testChannel-2";

        LinkedList<BlobInfoForTest> blobs = new LinkedList<>();
        blobs.add(new BlobInfoForTest(channelName1, Paths.get(fileName1)));
        blobs.add(new BlobInfoForTest(channelName2, Paths.get(fileName2)));

        // push response massage via test functionality
        link.next(SqlResponse.Response.newBuilder().build());

        // send request via product functionality
        var channelResponse = wire.send(SERVICE_ID_FOR_THE_TEST, toDelimitedByteArray(SqlRequest.Request.newBuilder().build()), blobs).get();

        // check the situation when the request has been sent
        var header = FrameworkRequest.Header.parseDelimitedFrom(new ByteArrayInputStream(link.getJustBeforeHeader()));
        assertTrue(header.hasBlobs());
        for (var e: header.getBlobs().getBlobsList()) {
            String channelName = e.getChannelName();
            String path = e.getPath();
            if (channelName.equals(channelName1)) {
                assertEquals(e.getPath(), fileName1);
            } else if (channelName.equals(channelName2)) {
                assertEquals(e.getPath(), fileName2);
            } else {
                fail("unexpected channel name " + channelName);
            }
        }
    }

    @Test
    void receiveBlob() throws Exception {
        // prepare a test file
        String fileName = "/tmp/channelFile-";
        fileName += Long.valueOf(ProcessHandle.current().pid()).toString();
        String channelName = "testChannel";
        String testData = "test data%n";
        try{
            FileWriter filewriter = new FileWriter(fileName);
            filewriter.write(testData);
            filewriter.close();
        } catch(IOException e){
            System.out.println(e);
        }

        // push response massage via test functionality
        var header = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(fileName)))
                        .build();
        link.next(header, SqlResponse.Response.newBuilder().build());

        // send request via product functionality
        var channelResponse = wire.send(SERVICE_ID_FOR_THE_TEST, toDelimitedByteArray(SqlRequest.Request.newBuilder().build())).get();

        // check the situation when the response is received
        try {
            var responsePayload = channelResponse.waitForMainResponse(10, TimeUnit.MILLISECONDS);
            var stream = channelResponse.openSubResponse(channelName, 10, TimeUnit.MILLISECONDS);

            // read the test file prepared
            var reader = new InputStreamReader(stream);
            var bufferedReader = new BufferedReader(reader);
            String content = bufferedReader.readLine();
            assertEquals(content, testData);
            bufferedReader.close();

            // delete the test file
            Files.delete(Paths.get(fileName));
        } catch (Exception e) {
            fail("response is not ready");
        }
    }

    @Test
    void primitiveFunctionOfMockLink() throws Exception {
        // push response massage via test functionality
        link.next(SqlResponse.Response.newBuilder()
                    .setBegin(SqlResponse.Begin.newBuilder()
                                .setSuccess(SqlResponse.Begin.Success.newBuilder()
                                                .setTransactionHandle(SqlCommon.Transaction.newBuilder()
                                                .setHandle(100))))
                    .build());

        // send request via product functionality
        var request = SqlRequest.Request.newBuilder().setBegin(SqlRequest.Begin.newBuilder()).build();
        var channelResponse = wire.send(SERVICE_ID_FOR_THE_TEST, toDelimitedByteArray(request)).get();

        // check the situation when the response is received
        try {
            var responsePayload = channelResponse.waitForMainResponse(10, TimeUnit.MILLISECONDS);
            var response = SqlResponse.Response.parseDelimitedFrom(new ByteBufferInputStream(responsePayload));
            var beginResponse = response.getBegin();
            assertEquals(SqlResponse.Begin.ResultCase.SUCCESS, beginResponse.getResultCase());
            assertEquals(100, beginResponse.getSuccess().getTransactionHandle().getHandle());
        } catch (Exception e) {
            fail("response is not ready");
        }
    }

    private byte[] toDelimitedByteArray(Message request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }

    @Test
    void sendToClosedLink() throws Exception {
        link.close();
        var payload = SqlRequest.Request.newBuilder().setBegin(SqlRequest.Begin.newBuilder()).build();
        var response = wire.send(SERVICE_ID_FOR_THE_TEST, toDelimitedByteArray(payload)).get();
        if (response instanceof ChannelResponse) {
            assertThrows(IOException.class, () -> {
                ((ChannelResponse)response).waitForMainResponse();
            });
        } else {
            fail("unexpected response type " + response.getClass().getName());
        }
    }
}
