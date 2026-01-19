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
package com.tsurugidb.tsubakuro.common.impl;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.common.BlobInfo;

/**
 * An implementation of {@link BlobInfo} that represents BLOB data stored in a file.
 */
public class FileBlobInfo implements BlobInfo {

    private final String channelName;
    private final Path path;

    /**
     * Creates a new instance.
     * @param channelName the channel name
     * @param path the path of the file that represents this BLOB data
     */
    public FileBlobInfo(@Nonnull String channelName, @Nonnull Path path) {
        Objects.requireNonNull(channelName);
        Objects.requireNonNull(path);
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

    // TODO: toString
}