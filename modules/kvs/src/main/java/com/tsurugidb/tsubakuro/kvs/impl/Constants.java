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

/**
 * The constant values.
 */
public final class Constants {

    /**
     * The service ID of KVS service.
     */
    public static final int SERVICE_ID_KVS = 5;

    /**
     * The scan result channel name.
     */
    public static final String SCAN_CHANNEL_NAME = "CURSOR"; //$NON-NLS-1$

    private Constants() {
        throw new AssertionError();
    }
}
