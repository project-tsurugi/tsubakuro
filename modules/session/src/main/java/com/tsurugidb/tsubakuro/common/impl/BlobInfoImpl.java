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

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.common.BlobInfo;
import com.tsurugidb.tsubakuro.common.BlobRelayReference;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;

/**
 * An implementation of {@link BlobInfo} that represents BLOB data stored in a file.
 */
public class BlobInfoImpl implements BlobInfo {

    private final String channelName;
    private final Path clientPath;
    private final LargeObjectInfo largeObjectInfo;

    /**
     * Creates a new instance.
     * @param channelName the channel name
     * @param clientPath the path of the file that represents this BLOB data
     */
    public BlobInfoImpl(@Nonnull String channelName, @Nonnull Path clientPath) {
        Objects.requireNonNull(channelName);
        Objects.requireNonNull(clientPath);
        this.channelName = channelName;
        this.clientPath = clientPath;
        this.largeObjectInfo = null;
    }

    /**
     * Creates a new instance.
     * @param channelName the channel name
     * @param largeObjectInfo the large object info that represents this BLOB data
     */
    public BlobInfoImpl(@Nonnull String channelName, @Nonnull LargeObjectInfo largeObjectInfo) {
        Objects.requireNonNull(channelName);
        Objects.requireNonNull(largeObjectInfo);
        this.channelName = channelName;
        this.clientPath = null;
        this.largeObjectInfo = largeObjectInfo;
    }

    @Override
    public String getChannelName() {
        return channelName;
    }

    @Override
    public boolean isFile() {
        return clientPath != null;
    }

    @Override
    public Optional<Path> getPath() {
        return Optional.ofNullable(clientPath);
    }

    @Override
    public Optional<String> getServerPath() {
        if (largeObjectInfo == null) {
            return Optional.empty();
        }
        if (largeObjectInfo.getInfoType() != LargeObjectInfo.InfoType.SERVER_PATH) {
            return Optional.empty();
        }
        return Optional.ofNullable(largeObjectInfo.getServerPath());
    }

    @Override
    public Optional<BlobRelayReference> getBlobRelayReference() {
        if (largeObjectInfo == null) {
            return Optional.empty();
        }
        if (largeObjectInfo.getInfoType() != LargeObjectInfo.InfoType.BLOB_RELAY_REFERENCE) {
            return Optional.empty();
        }
        return Optional.ofNullable(largeObjectInfo.getBlobRelayReference());
    }

    // TODO: toString
}