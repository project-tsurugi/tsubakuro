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
package com.tsurugidb.tsubakuro.common;

/**
 * An abstract super interface of large object references at Session layer.
 *
 * @since 1.10.0
 */
public interface LargeObjectReferenceBase {
    /**
     * Returns the provider of the LOB data.
     * @return the provider value
     */
    long getProvider();

    /**
     * Returns the object id of the LOB data.
     * @return the object id value
     */
    long getObjectId();

    /**
     * Returns the reference tag of the LOB data.
     * @return the reference tag value
     */
    long getReferenceTag();
}
