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
package com.tsurugidb.tsubakuro.sql;

import java.nio.file.Path;
import java.util.Optional;

import com.tsurugidb.tsubakuro.util.ServerResource;

/**
 * Represents large object cache.
 */
public interface LargeObjectCache extends ServerResource {

    /**
     * Returns the path of the file that represents the large object, only if it exists.
     * The returned Path is available until close() of this object is invoked.
     * @return the path of the file, or empty if it does not exist
     */
    Optional<Path> find();
}
