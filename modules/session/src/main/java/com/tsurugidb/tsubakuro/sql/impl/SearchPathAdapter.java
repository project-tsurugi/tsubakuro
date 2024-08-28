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
