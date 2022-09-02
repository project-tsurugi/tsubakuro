package com.tsurugidb.tsubakuro.datastore;

import java.io.IOException;
import java.util.Collection;
import com.tsurugidb.datastore.proto.DatastoreCommon;

/**
 * TagList type.
 */
public interface TagList {
    /**
     * Get a list of file path
     * @return List of recovery tagr
     * @throws IOException error occurred in listing backup files
     */
    Collection<DatastoreCommon.Tag> tags() throws IOException;
}
