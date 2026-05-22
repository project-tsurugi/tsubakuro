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

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.common.LargeObjectCache;
import com.tsurugidb.tsubakuro.common.LargeObjectClient;
import com.tsurugidb.tsubakuro.common.LargeObjectInfo;
import com.tsurugidb.tsubakuro.common.LargeObjectReference;
import com.tsurugidb.tsubakuro.common.exception.BlobException;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * A void/disabled implementation of {@link LargeObjectClient}.
 * This client represents the absence of Large Object (BLOB) support in the current session,
 * such as when the session is configured not to use a Large Object transfer medium
 * (for example, {@code DOES_NOT_USE} / no medium).
 * All operations are unavailable and will throw {@link IllegalStateException}.
 *
 * @since 1.11.0
 */
public class LargeObjectClientVoid implements LargeObjectClient {
    static final String ERROR_MESSAGE = "BLOB functionality is unavailable in this session."; //$NON-NLS-1$

    @Override
    public FutureResponse<LargeObjectInfo> upload(InputStream source) throws BlobException {
        throw new IllegalStateException(ERROR_MESSAGE);
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Reader source) throws BlobException {
        throw new IllegalStateException(ERROR_MESSAGE);
    }

    @Override
    public FutureResponse<LargeObjectInfo> upload(Path source) throws IOException {
        throw new IllegalStateException(ERROR_MESSAGE);
    }

    @Override
    public FutureResponse<InputStream> openInputStream(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        throw new IllegalStateException(ERROR_MESSAGE);
    }

    @Override
    public FutureResponse<Reader> openReader(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        throw new IllegalStateException(ERROR_MESSAGE);
    }

    @Override
    public FutureResponse<LargeObjectCache> getLargeObjectCache(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref) throws BlobException {
        throw new IllegalStateException(ERROR_MESSAGE);
    }

    @Override
    public FutureResponse<Void> copyTo(@Nonnull ContextId contextId, @Nonnull LargeObjectReference ref, @Nonnull Path destination) throws BlobException {
        throw new IllegalStateException(ERROR_MESSAGE);
    }
}