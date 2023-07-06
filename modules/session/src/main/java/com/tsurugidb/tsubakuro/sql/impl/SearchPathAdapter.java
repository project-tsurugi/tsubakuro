package com.tsurugidb.tsubakuro.sql.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.sql.SearchPath;
import com.tsurugidb.sql.proto.SqlResponse;

/**
 * An implementation of {@link TableList} which just wraps original protocol buffers' message.
 */
public class SearchPathAdapter implements SearchPath {

    private final SqlResponse.GetSearchPath.Success proto;

    /**
     * Creates a new instance.
     * @param proto the corresponding protocol buffers message
     */
    public SearchPathAdapter(@Nonnull SqlResponse.GetSearchPath.Success proto) {
        Objects.requireNonNull(proto);
        this.proto = proto;
    }

    @Override
    public List<String> getSchemaNames() {
        var rv = new ArrayList<String>();
        for (var e : proto.getSearchPathsList()) {
            var l = e.getIdentifiersList();
            if (l.size() > 0) {
                String name = l.get(0).getLabel();
                for (int i = 1; i < l.size(); i++) {
                    name += ".";
                    name += l.get(i).getLabel();
                }
                rv.add(name);
            }
        }
        return rv;
    }

    @Override
    public String toString() {
        return proto.toString();
    }
}
