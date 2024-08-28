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
package com.tsurugidb.tsubakuro.kvs.impl;

import java.text.MessageFormat;

import com.tsurugidb.tsubakuro.kvs.PutResult;

/**
 * An implementation of {@link PutResult}.
 */
public class PutResultImpl implements PutResult {

    private final int written;

    /**
     * Creates a new instance.
     * @param written the number of records which the operation actually written.
     */
    public PutResultImpl(int written) {
        if (written < 0) {
            throw new IllegalArgumentException(
                    MessageFormat.format("written count is negative: {}", written));
        }
        this.written = written;
    }

    @Override
    public int size() {
        return written;
    }

}
