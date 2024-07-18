package com.tsurugidb.tsubakuro.common.impl;

import java.util.LinkedHashSet;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

/**
 * Keeps {@link ServerResource}s.
 */
class ServiceShelf {

    static final Logger LOG = LoggerFactory.getLogger(ServiceShelf.class);

    private final LinkedHashSet<ServerResource> entries = new LinkedHashSet<>();

    /**
     * Put a {@link ServerResource} to this.
     * @param resource the resource
     * @return the input resource
     */
    void put(@Nonnull ServerResource resource) {
        synchronized (this) {
            entries.add(resource);
        }
    }

    /**
     * Unregisters a {@link ServerResource} from this.
     * If such the object is not registered, this does nothing.
     */
    boolean remove(@Nonnull ServerResource resource) {
        synchronized (this) {
            return entries.remove(resource);
        }
    }

    void setCloseTimeout(@Nonnull Timeout timeout) {
        synchronized (this) {
            for (var iter = entries.iterator(); iter.hasNext();) {
                var resource = iter.next();
                resource.setCloseTimeout(timeout);
            }
        }
    }

    LinkedHashSet<ServerResource> entries() {
        return entries;
    }

    void forEach(java.util.function.Consumer<ServerResource> f) {
        for (var e : entries) {
            f.accept(e);
        }
    }
}
