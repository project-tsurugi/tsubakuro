/*
 * Copyright 2023-2025 Project Tsurugi.
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.tsurugidb.framework.proto.FrameworkCommon;
import com.tsurugidb.framework.proto.FrameworkResponse;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.mock.MockLink;

class ChannelResponseLobTest {
    MockLink link = new MockLink();

    private final String channelName = "channelName";
    private final String fileName = "lob.data";

    @Test
    void testOpenSubResponseSuccess(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve(fileName);
        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Files.write(file, data);

        var responseHeader = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(file.toString())));
        link.next(responseHeader.build(), SqlRequest.Request.newBuilder().build());

        byte[] requestHeader = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };  // dummy request header
        byte[] payload = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }; // dummy payload
        ChannelResponse channelResponse = link.send(requestHeader, payload);
        var inputStream = channelResponse.openSubResponse(channelName);

        assertTrue(inputStream instanceof ChannelResponse.FileInputStreamWithPath);
        assertEquals(((ChannelResponse.FileInputStreamWithPath) inputStream).path().toString(), file.toString());

        byte[] obtainedData = new byte[data.length];
        inputStream.read(obtainedData);
        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], obtainedData[i]);
        }
    }

    @Test
    void testOpenSubResponseDifferentChannelName(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve(fileName);
        byte[] data = new byte[] { 0x01, 0x02, 0x03 };
        Files.write(file, data);

        var responseHeader = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(file.toString())));
        link.next(responseHeader.build(), SqlRequest.Request.newBuilder().build());

        byte[] requestHeader = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };  // dummy request header
        byte[] payload = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }; // dummy payload
        ChannelResponse channelResponse = link.send(requestHeader, payload);
        assertThrows(NoSuchElementException.class, () -> channelResponse.openSubResponse(channelName + "different"));
    }

    @Test
    void testOpenSubResponseFileDoesNotExist(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve(fileName);

        var responseHeader = FrameworkResponse.Header.newBuilder()
                        .setPayloadType(FrameworkResponse.Header.PayloadType.SERVICE_RESULT)
                        .setBlobs(FrameworkCommon.RepeatedBlobInfo.newBuilder()
                                    .addBlobs(FrameworkCommon.BlobInfo.newBuilder()
                                                .setChannelName(channelName)
                                                .setPath(file.toString())));
        link.next(responseHeader.build(), SqlRequest.Request.newBuilder().build());

        byte[] requestHeader = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };  // dummy request header
        byte[] payload = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }; // dummy payload
        ChannelResponse channelResponse = link.send(requestHeader, payload);

        assertThrows(IOException.class, () -> channelResponse.openSubResponse(channelName));
    }
}
