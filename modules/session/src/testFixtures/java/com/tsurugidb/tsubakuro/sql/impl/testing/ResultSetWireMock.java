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
package com.tsurugidb.tsubakuro.sql.impl.testing;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.util.ByteBufferInputStream;

public class ResultSetWireMock implements ResultSetWire {
    private ByteBufferInputStream byteBufferInput;
    private ByteBuffer buf;

    public ResultSetWireMock(ByteBuffer buf) {
        byteBufferInput = null;
        this.buf = buf;
    }

    public ResultSetWireMock(byte[] ba) {
        this(ByteBuffer.wrap(ba));
    }

    @Override
    public ByteBufferInputStream getByteBufferBackedInput() {
        if (byteBufferInput == null) {
            byteBufferInput = new ByteBufferInputStream(buf);
        }
        return byteBufferInput;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}