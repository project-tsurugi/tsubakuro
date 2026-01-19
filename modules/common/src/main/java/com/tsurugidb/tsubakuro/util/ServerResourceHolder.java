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
package com.tsurugidb.tsubakuro.util;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * Holds {@link ServerResource}s and closes them.
 */
public class ServerResourceHolder implements ServerResource, ServerResource.CloseHandler {

    static final Logger LOG = LoggerFactory.getLogger(ServerResourceHolder.class);

    private final ConcurrentHashMap<IdentityProvider, Boolean> entries = new ConcurrentHashMap<>();

    /**
     * Registers a {@link ServerResource} to this.
     * The registered object will be closed in {@link ServerResourceHolder#close()}.
     * @param <T> the resource type
     * @param resource the resource
     * @return the input resource
     */
    public <T extends ServerResource> T register(@Nullable T resource) {
        if (resource != null) {
            entries.put(new IdentityProvider(resource), Boolean.TRUE); // dummy value
        }
        return resource;
    }

    /**
     * Unregisters a {@link ServerResource} from this.
     * If such the object is not registered, this does nothing.
     */
    @Override
    public void onClosed(ServerResource resource) {
        if (resource != null) {
            entries.remove(new IdentityProvider(resource));
        }
    }

    @Override
    public void setCloseTimeout(@Nonnull Timeout timeout) {
        for (var iter = entries.entrySet().iterator(); iter.hasNext();) {
            var resource = iter.next().getKey().resource;
            resource.setCloseTimeout(timeout);
        }
    }

    @Override
    public void close() throws IOException, ServerException, InterruptedException {
        for (var iter = entries.entrySet().iterator(); iter.hasNext();) {
            try (var resource = iter.next().getKey().resource) {
                LOG.trace("cleanup: {}", resource); //$NON-NLS-1$
                iter.remove();
            } catch (IOException | ServerException e) {
                LOG.warn("error suppressed during cleanup", e);
            }
        }
    }

    /**
     * Applies the given function to each registered resource.
     * @param f the function
     */
    public void forEach(java.util.function.Consumer<ServerResource> f) {
        for (var iter = entries.entrySet().iterator(); iter.hasNext();) {
            f.accept(iter.next().getKey().resource);
        }
    }

    static class IdentityProvider {

        final ServerResource resource;

        IdentityProvider(ServerResource resource) {
            this.resource = resource;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(resource);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            IdentityProvider other = (IdentityProvider) obj;
            return resource == other.resource;
        }

        @Override
        public String toString() {
            return Objects.toString(resource);
        }
    }
}
