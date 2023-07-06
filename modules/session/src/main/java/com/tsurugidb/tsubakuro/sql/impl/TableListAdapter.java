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
        var rv = new ArrayList<String>();
        for (var n : proto.getTablePathNamesList()) {
            if (n.getIdentifiersList().size() > 0) {
                rv.add(getTableName(n));
            }
        }
        return rv;
    }

    @Override
    public List<String> getSimpleNames(SearchPath searchPath) {
        var rv = new ArrayList<String>();
        for (var n : proto.getTablePathNamesList()) {
            if (n.getIdentifiersList().size() > 0) {
                var sn = searchPath.getSchemaNames();
                boolean find = true;
                for (int i = 0; i < sn.size(); i++) {
                    if (!sn.get(i).equals(n.getIdentifiersList().get(i).getLabel())) {
                        find = false;
                        break;
                    }
                }
                if (find) {
                    rv.add(getTableName(n));
                }
            }
        }
        return rv;
    }

    private String getTableName(SqlResponse.Name n) {
        var l = n.getIdentifiersList();
        if (l.size() > 0) {
            String name = l.get(0).getLabel();
            for (int i = 1; i < l.size(); i++) {
                name += ".";
                name += l.get(i).getLabel();
            }
            return name;
        }
        return "";  // never happen
    }

    @Override
    public String toString() {
        return proto.toString();
    }
}
