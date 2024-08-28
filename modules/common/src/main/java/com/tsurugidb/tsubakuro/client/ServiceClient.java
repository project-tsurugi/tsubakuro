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
package com.tsurugidb.tsubakuro.client;

/**
 * A marker interface for clients of services on tsurugidb.
 *
 * <p>
 * The followings are RECOMMENDED for each service client that inherits this:
 * </p>
 * <ul>
 * <li> Put {@link ServiceMessageVersion} annotation to the client declaration. </li>
 * <li> Add class name to the service client definition file (defined in {@link ServiceClientCollector}). </li>
 * </ul>
 * @see ServiceMessageVersion
 * @see ServiceClientCollector
 */
public interface ServiceClient {
    // no special members
}
