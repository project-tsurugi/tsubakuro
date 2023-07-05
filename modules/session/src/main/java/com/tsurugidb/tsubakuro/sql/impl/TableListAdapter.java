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
public class TableListAdapter implements TableList {

    private final SqlResponse.ListTables.Success proto;

    /**
     * Creates a new instance.
     * @param proto the corresponding protocol buffers message
     */
    public TableListAdapter(@Nonnull SqlResponse.ListTables.Success proto) {
        Objects.requireNonNull(proto);
        this.proto = proto;
    }

    @Override
    public List<String> getTableNames() {
        var l = new ArrayList<String>();
        for (var e : proto.getTablePathNamesList()) {
            l.add(e.getTableName().getLabel());
        }
        return l;
    }

    @Override
    public List<String> getSimpleNames(SearchPath searchPath) {
        var l = new ArrayList<String>();
        for (var e : proto.getTablePathNamesList()) {
            for (var n : searchPath.getSchemaNames()) {
                if (n.equals(e.getSchemaName().getLabel())) {
                    l.add(e.getTableName().getLabel());
                }
            }
        }
        return l;
    }

    @Override
    public String toString() {
        return proto.toString();
    }
}
