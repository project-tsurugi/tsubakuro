/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.tsubakuro.common.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Path;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.common.BlobPathMapping;
import com.tsurugidb.tsubakuro.common.LargeObjectCache;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.common.LargeObjectReference;
import com.tsurugidb.tsubakuro.common.exception.BlobException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * An implementation of {@link LargeObjectClient} that provides privileged access to the Large Object storage.
 * This client is used when the endpoint supports privileged access and the session is configured to use privileged transfer type.
 *
 * @since 1.11.0
 */
public class LargeObjectClientPrivileged implements LargeObjectClient {
    private final BlobPathMapping blobPathMapping;

    /**
     * Creates a new instance.
     * @param blobPathMapping the blob path mapping
     */
    public LargeObjectClientPrivileged(BlobPathMapping blobPathMapping) {
        this.blobPathMapping = blobPathMapping;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(InputStream source) throws BlobException {
        // Implementation for privileged mode upload from InputStream
        return null;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Reader source) throws BlobException {
        // Implementation for privileged mode upload from Reader
        return null;
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Path source) throws IOException {
        var info = SqlRequest.ClientOnlyLargeObjectInfo.newBuilder();
        if (blobPathMapping != null) {
            var mapping = blobPathMapping.getOnSend();
            if (mapping != null) {
                for (var entry : mapping) {
                    var cp = entry.getClientPath();
                    var clientPath = cp.isAbsolute() ? cp : cp.toAbsolutePath();
                    if (source.startsWith(clientPath)) {
                        var remainingPath = source.subpath(entry.getClientPath().getNameCount(), source.getNameCount());
                        if (remainingPath != null) {
                            String serverPath = entry.getServerPath();
                            for (int i = 0; i < remainingPath.getNameCount(); i++) {
                                serverPath += "/" + remainingPath.getName(i).toString();  // server path is separated by "/"
                            }
                            info.setServerPath(serverPath);
                            LargeObjectInfo largeObjectInfo = new LargeObjectInfoImpl(info.build());
                            return FutureResponse.returns(largeObjectInfo);
                        }
                    }
                }
            }
        }        // If no mapping is found, treat the path as a server path directly
        info.setServerPath(source.toString());
        LargeObjectInfo largeObjectInfo = new LargeObjectInfoImpl(info.build());
        return FutureResponse.returns(largeObjectInfo);
    }

    @Override
    public FutureResponse<InputStream> openInputStream(ContextId contextId, LargeObjectReference ref) throws BlobException {
        throw new UnsupportedOperationException("openInputStream is not supported in privileged mode");
    }

    @Override
    public FutureResponse<Reader> openReader(ContextId contextId, LargeObjectReference ref) throws BlobException {
        throw new UnsupportedOperationException("openReader is not supported in privileged mode");
    }

    @Override
    public FutureResponse<LargeObjectCache> getLargeObjectCache(ContextId contextId, LargeObjectReference ref) throws BlobException {
        throw new UnsupportedOperationException("getLargeObjectCache is not supported in privileged mode");
    }

    @Override
    public FutureResponse<Void> copyTo(ContextId contextId, LargeObjectReference ref, Path destination) throws BlobException {
        throw new UnsupportedOperationException("copyTo is not supported in privileged mode");
    }
}