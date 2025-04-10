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
package com.tsurugidb.tsubakuro.common;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Represents a BLOB path mapping for BLOB privileged mode.
 */
public class BlobPathMapping {

    /**
     * Represents an entry of BLOB path mapping.
     */
    public static class Entry {

        private final Path clientPath;
        private final String serverPath;

        Entry(@Nonnull Path clientPath, @Nonnull String serverPath) {
            Objects.requireNonNull(clientPath);
            Objects.requireNonNull(serverPath);
            this.clientPath = clientPath;
            this.serverPath = serverPath;
        }

        /**
         * Returns the client path, must be a directory.
         * @return the client path
         */
        public Path getClientPath() {
            return clientPath;
        }

        /**
         * Returns the server path, must be a directory.
         * @return the server path
         */
        public String getServerPath() {
            return serverPath;
        }

        @Override
        public int hashCode() {
            int result = 17;
            final int prime = 31;

            result = prime * result + Objects.hashCode(clientPath);
            result = prime * result + Objects.hashCode(serverPath);

            return result;
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
            Entry other = (Entry) obj;
            return Objects.equals(clientPath, other.clientPath) && Objects.equals(serverPath, other.serverPath);
        }

        @Override
        public String toString() {
            return String.format("clientPath(%s) - serverPath(%s)", clientPath.toString(), serverPath); //$NON-NLS-1$
        }
    }

    /**
     * Represents a builder for {@link BlobPathMapping}.
     */
    public static class Builder {

        private final List<Entry> builderOnSend = new ArrayList<>();
        private final List<Entry> builderOnReceive = new ArrayList<>();

        /**
         * Adds a path mapping entry for sending BLOBs.
         * @param clientPath the client path to be transformed, must be a directory
         * @param serverPath the target server path, must be a directory
         * @return this
         */
        public Builder onSend(@Nonnull Path clientPath, @Nonnull String serverPath) {
            Objects.requireNonNull(clientPath);
            Objects.requireNonNull(serverPath);
            builderOnSend.add(new Entry(clientPath, serverPath));
            return this;
        }

        /**
         * Adds a path mapping entry for receiving BLOBs.
         * @param serverPath the target server path to be transformed, must be a directory
         * @param clientPath the target client path, must be a directory
         * @return this
         */
        public Builder onReceive(@Nonnull String serverPath, @Nonnull Path clientPath) {
            Objects.requireNonNull(clientPath);
            Objects.requireNonNull(serverPath);
            builderOnReceive.add(new Entry(clientPath, serverPath));
            return this;
        }

        /**
         * Adds a path mapping entry for both sending and receiving BLOBs.
         * @param clientPath the client path, must be a directory
         * @param serverPath the server path, must be a directory
         * @return this
         */
        public Builder onBoth(@Nonnull Path clientPath, @Nonnull String serverPath) {
            Objects.requireNonNull(clientPath);
            Objects.requireNonNull(serverPath);
            builderOnSend.add(new Entry(clientPath, serverPath));
            builderOnReceive.add(new Entry(clientPath, serverPath));
            return this;
        }

        /**
         * Builds a {@link BlobPathMapping} instance.
         * @return the built instance
         */
        public BlobPathMapping build() {
            return new BlobPathMapping(builderOnSend, builderOnReceive);
        }
    }

    private final List<Entry> onSend;
    private final List<Entry> onReceive;

    /**
     * Creates a new empty instance.
     */
    public BlobPathMapping() {
        this(List.of(), List.of());
    }

    /**
     * Creates a new instance.
     * @param onSend the list of path mapping entries for sending BLOBs
     * @param onReceive the list of path mapping entries for receiving BLOBs
     * @see #newBuilder()
     */
    public BlobPathMapping(@Nonnull List<? extends Entry> onSend, @Nonnull List<? extends Entry> onReceive) {
        Objects.requireNonNull(onSend);
        Objects.requireNonNull(onReceive);
        this.onSend = List.copyOf(onSend);
        this.onReceive = List.copyOf(onReceive);
    }

    /**
     * Creates a new builder.
     * @return the new builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Returns the list of path mapping entries for sending BLOBs.
     * <p>
     * Path transformation will be applied from the head of the list, and the first matched entry will be used.
     * </p>
     * @return the list of path mapping entries for sending BLOBs
     */
    public List<Entry> getOnSend() {
        return onSend;
    }

    /**
     * Returns the list of path mapping entries for receiving BLOBs.
     * <p>
     * Path transformation will be applied from the head of the list, and the first matched entry will be used.
     * </p>
     * @return the list of path mapping entries for receiving BLOBs
     */
    public List<Entry> getOnReceive() {
        return onReceive;
    }

    @Override
    public int hashCode() {
        int result = 17;
        final int prime = 31;

        result = prime * result + Objects.hashCode(onSend);
        result = prime * result + Objects.hashCode(onReceive);

        return result;
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
        BlobPathMapping other = (BlobPathMapping) obj;
        return Objects.equals(onSend, other.onSend) && Objects.equals(onReceive, other.onReceive);
    }

    @Override
    public String toString() {
        return "onReceive\n" + onReceive.stream().map(Entry::toString).reduce(" ", (a, b) -> a + b)
             + "\nonSend\n" + onSend.stream().map(Entry::toString).reduce(" ", (a, b) -> a + b); //$NON-NLS-1$
    }
}