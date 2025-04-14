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
package com.tsurugidb.tsubakuro.sql.impl;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.sql.LargeObjectCache;
import com.tsurugidb.tsubakuro.util.Lang;
import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * An implementation of {@link LargeObjectCache}.
 */
public class LargeObjectCacheImpl implements LargeObjectCache {

    static final Logger LOG = LoggerFactory.getLogger(LargeObjectCacheImpl.class);

    private final CloseHandler closeHandler;

    private final Path path;

    private final boolean exists;

    private final AtomicBoolean closed = new AtomicBoolean();

    public LargeObjectCacheImpl(@Nullable ServerResource.CloseHandler closeHandler, @Nullable Path path) {
        this.closeHandler = closeHandler;
        this.path = path;
        if (path != null) {
            exists = new File(path.toString()).exists();
            return;
        }
        exists = false;
    }
    public LargeObjectCacheImpl(@Nullable ServerResource.CloseHandler closeHandler) {
        this.closeHandler = closeHandler;
        this.path = null;
        exists = false;
    }

    @Override
    public Optional<Path> find() {
        if (closed.get() || !exists) {
            return Optional.empty();
        }
        return Optional.of(path);
    }

    @Override
    public void close() {
        closed.set(true);
        if (closeHandler != null) {
            Lang.suppress(
                    e -> LOG.warn("error occurred while collecting garbage", e),
                    () -> closeHandler.onClosed(this));
        }
    }
}
