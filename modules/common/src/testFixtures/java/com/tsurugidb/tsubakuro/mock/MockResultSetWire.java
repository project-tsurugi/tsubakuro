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
package com.tsurugidb.tsubakuro.mock;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;

/**
 * ResultSetWire type.
 */
public class MockResultSetWire implements ResultSetWire {
    private boolean closed;

    class ByteBufferBackedInputForTest extends ByteBufferBackedInput {
        ByteBufferBackedInputForTest() {
        }

        @Override
        protected boolean next() {
            return false;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    MockResultSetWire() {
        this.closed = false;
    }

    @Override
    public ResultSetWire connect(String name) {
        return this;
    }

    @Override
    public InputStream getByteBufferBackedInput() {
        return new ByteBufferBackedInputForTest();
    }

    @Override
    public void close() {
    }

    public boolean isClosed() {
        return closed;
    }
}
