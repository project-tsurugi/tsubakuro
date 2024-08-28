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
package com.tsurugidb.tsubakuro.kvs;

/**
 * Represents a behavior of {@code COMMIT} operation.
 */
public enum CommitType {

    /**
     * Commits the transaction with using the server settings.
     */
    UNSPECIFIED,

    /**
     * Commits the transaction and wait for the operation has accepted.
     * <ul>
     * <li> The transaction will never abort after this operation was completed, except system errors. </li>
     * <li> The committed data may or may not be visible for the other transactions. </li>
     * <li> System errors may lost the committed data. </li>
     * </ul>
     */
    ACCEPTED,

    /**
     * Commits the transaction and wait for the committed data have been visible for other transactions.
     * <ul>
     * <li> The transaction will never abort after this operation was completed, except system errors. </li>
     * <li> The committed data are visible for the other transactions after this operation was completed. </li>
     * <li> System errors may lost the committed data. </li>
     * </ul>
     */
    AVAILABLE,

    /**
     * Commits the transaction and wait for the committed data have been saved on the local disk.
     * <ul>
     * <li> The transaction will never abort after this operation was completed, except system errors. </li>
     * <li> The committed data are visible for the other transactions after this operation was completed. </li>
     * <li> System errors never lost the committed data only if the local disks are available. </li>
     * </ul>
     */
    STORED,

    /**
     * Commits the transaction and wait for the committed data have been propagated to the all suitable nodes.
     * <p>
     * Note that, this is equivalent to {@link #STORED} if the database does not have any other replicas.
     * </p>
     * <ul>
     * <li> The transaction will never abort after this operation was completed, except system errors. </li>
     * <li> The committed data are visible for the other transactions after this operation was completed. </li>
     * <li> System errors never lost the committed data. </li>
     * </ul>
     */
    PROPAGATED,

    ;

    /**
     * The default behavior of {@code COMMIT} operation.
     * @see #UNSPECIFIED
     */
    public static final CommitType DEFAULT_BEHAVIOR = UNSPECIFIED;
}
