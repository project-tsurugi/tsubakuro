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
package com.tsurugidb.tsubakuro.auth;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.CoreServiceException;

/**
 * An interface to provide authentication {@link Ticket}.
 */
public interface TicketProvider {

    /**
     * Restores {@link Ticket} from its {@link Ticket#getToken() token text}.
     * <p>
     * The returned ticket may not have access permissions.
     * If {@link Ticket#getAccessExpirationTime()} returns empty, please invoke
     * {@link #refresh(Ticket, long, TimeUnit) refresh()} consequently to the returned one to gain access permissions.
     * </p>
     * @param text the token text
     * @return the restored authentication token
     * @throws IllegalArgumentException if the token text is ill-formed
     */
    Ticket restore(@Nonnull String text);

    /**
     * Obtains a new {@link Ticket} using a pair of user ID and its password.
     * <p>
     * The returned ticket may not have access permissions.
     * If {@link Ticket#getAccessExpirationTime()} returns empty, please invoke
     * {@link #refresh(Ticket, long, TimeUnit) refresh()} consequently to the returned one to gain access permissions.
     * </p>
     * @param userId the user ID
     * @param password the password
     * @return the authentication token
     * @throws InterruptedException if interrupted while requesting authentication to the service
     * @throws IOException if I/O error was occurred while requesting authentication to the service
     * @throws CoreServiceException if authentication was failed
     */
    Ticket issue(@Nonnull String userId, @Nonnull String password) throws InterruptedException, IOException, CoreServiceException;

    /**
     * Requests to extend access expiration time of the given {@link Ticket}.
     * <p>
     * This operation requires which the {@link Ticket#getRefreshExpirationTime() refresh expiration time} is
     * remained in the given ticket.
     * </p>
     * <p>
     * This never modifies the input {@link Ticket} object.
     * </p>
     * @param ticket the old {@link Ticket} to refresh access expiration time
     * @param expiration the maximum time to extend the access expiration from now,
     *      or {@code <= 0} to use default expiration time
     * @param unit the time unit of expiration
     * @return the refreshed {@link Ticket}
     * @throws IllegalArgumentException if the input ticket is not provided by this object
     * @throws IllegalArgumentException if the input ticket does not support
     *      {@link Ticket#getRefreshExpirationTime() refresh}
     * @throws InterruptedException if interrupted while requesting authentication to the service
     * @throws IOException if I/O error was occurred while requesting authentication to the service
     * @throws CoreServiceException if refresh operation was failed in the authentication mechanism
     */
    Ticket refresh(
            @Nonnull Ticket ticket,
            long expiration,
            @Nonnull TimeUnit unit) throws InterruptedException, IOException, CoreServiceException;

    /**
     * Requests to extend access expiration time of the given {@link Ticket}.
     * <p>
     * This operation is equivalent to {@link #refresh(Ticket, long, TimeUnit) refresh(ticket, 0, TimeUnit.SECONDS)}.
     * </p>
     * <p>
     * This operation requires which the {@link Ticket#getRefreshExpirationTime() refresh expiration time} is
     * remained in the given ticket.
     * </p>
     * <p>
     * This never modifies the input {@link Ticket} object.
     * </p>
     * @param ticket the old {@link Ticket} to refresh access expiration time
     * @return the refreshed {@link Ticket}
     * @throws IllegalArgumentException if the input ticket is not provided by this object
     * @throws IllegalArgumentException if the input ticket does not support
     *      {@link Ticket#getRefreshExpirationTime() refresh}
     * @throws InterruptedException if interrupted while requesting authentication to the service
     * @throws IOException if I/O error was occurred while requesting authentication to the service
     * @throws CoreServiceException if refresh operation was failed in the authentication mechanism
     */
    default Ticket refresh(@Nonnull Ticket ticket) throws InterruptedException, IOException, CoreServiceException {
        return refresh(ticket, 0, TimeUnit.SECONDS);
    }
}
