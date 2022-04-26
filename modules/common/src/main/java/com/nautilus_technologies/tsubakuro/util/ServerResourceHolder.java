package com.nautilus_technologies.tsubakuro.util;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.nautilus_technologies.tsubakuro.exception.ServerException;

/**
 * Holds {@link ServerResource}s and closes them in {@link ServerResourceHolder#close()}.
 */
public class ServerResourceHolder implements ServerResource, ServerResource.CloseHandler {

    private final ConcurrentHashMap<IdentityProvider, Boolean> entries = new ConcurrentHashMap<>();

    /**
     * Registers a {@link ServerResource} to this.
     * The registered object will be closed in {@link ServerResourceHolder#close()}.
     * @param <T> the resource type
     * @param resource the resource
     * @return the input resource
     */
    public <T extends ServerResource> T register(@Nullable T resource) {
        if (Objects.nonNull(resource)) {
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
        if (Objects.nonNull(resource)) {
            entries.remove(new IdentityProvider(resource));
        }
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        for (var iter = entries.entrySet().iterator(); iter.hasNext();) {
            iter.next().getKey().resource.close();
            iter.remove();
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
