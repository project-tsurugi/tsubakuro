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

import com.tsurugidb.tsubakuro.sql.LargeObjectCache;

/**
 * An implementation of {@link LargeObjectCache}.
 */
public class LargeObjectCacheImpl implements LargeObjectCache {

    private final Path path;
    private final boolean exists;
    private final AtomicBoolean closed = new AtomicBoolean();
    
    public LargeObjectCacheImpl(@Nullable Path path) {
        this.path = path;
        if (path != null) {
            exists = new File(path.toString()).exists();
            return;
        }
        exists = false;
    }
    public LargeObjectCacheImpl() {
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
    }
}
