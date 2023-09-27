package com.tsurugidb.tsubakuro.kvs.compatibility;

import com.tsurugidb.kvs.proto.KvsData;
import com.tsurugidb.tsubakuro.kvs.Values;

class SqlValue {

    final KvsData.Value value;
    final String str;

    SqlValue(String typeName, KvsData.Value value) {
        this.value = value;
        this.str = toSqlString(typeName, value);
    }

    private static String toSqlString(String typeName, KvsData.Value v) {
        Object o = Values.toObject(v);
        if (o == null) {
            return "NULL";
        }
        String s = o.toString();
        switch (v.getValueCase()) {
        case FLOAT4_VALUE:
        case DECIMAL_VALUE:
            return String.format("cast('%s' as %s)", s, typeName);
        case CHARACTER_VALUE:
            return String.format("'%s'", s);
        default:
            return s;
        }
    }

}
