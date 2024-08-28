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
package com.tsurugidb.tsubakuro.auth.http;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.exception.CoreServiceException;

/**
 * A service interface that provides authentication tokens..
 */
public interface TokenProvider {

    /**
     * Requests to issue a new refresh token.
     * @param user the user name
     * @param password the password
     * @return the issued refresh token
     * @throws InterruptedException if interrupted while requesting a new token
     * @throws IOException if I/O error was occurred while requesting a new token
     * @throws CoreServiceException if authentication was failed, or the retrieved token was something wrong
     */
    String issue(@Nonnull String user, @Nonnull String password)
            throws InterruptedException, IOException, CoreServiceException;

    /**
     * Requests to issue a new access token.
     * @param token the refresh token
     * @param expiration the maximum time to extend the access expiration from now
     * @param unit the time unit of expiration
     * @return the issued access token
     * @throws InterruptedException if interrupted while requesting a new token
     * @throws IOException if I/O error was occurred while requesting a new token
     * @throws CoreServiceException if authentication was failed, or the retrieved token was something wrong
     */
    String refresh(@Nonnull String token, long expiration, @Nonnull TimeUnit unit)
            throws InterruptedException, IOException, CoreServiceException;

    /**
     * Requests to verify the given token.
     * Note that, this never considers whether the token was expired.
     * @param token the target token
     * @throws InterruptedException if interrupted while verifying the token
     * @throws IOException if I/O error was occurred while verifying the token
     * @throws CoreServiceException if the token was ill-formed
     */
    void verify(@Nonnull String token)
            throws InterruptedException, IOException, CoreServiceException;
}
