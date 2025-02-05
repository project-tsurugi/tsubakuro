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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Represents large object cache.
 */
public interface LargeObjectCache extends AutoCloseable {

    /**
     * Returns the path of the file that represents the large object, only if it exists.
     * The returned Path is available until close() of this object is invoked.
     * @return the path of the file, or empty if it does not exist
     */
    Optional<Path> find();

    /**
     * Copy the large object to the file indicated by the given path.
     * Files created by this method are not affected by close() of this object. 
     * @param destination the path of the destination file
     * @throws IOException if I/O error was occurred while copying the large object to the file
     * @throws IllegalStateException encountering a situation where find() returns an empty Optional
     */
    void copyTo(Path destination) throws IOException;

    /**
     * Closes the object cache. The file whose Path has been returned by find() may be deleted.
     */
    @Override
    void close();
}
