package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.low.sql.Placeholders;
import com.nautilus_technologies.tsubakuro.low.sql.Types;
import com.tsurugidb.jogasaki.proto.SqlCommon.AtomType;
import com.tsurugidb.jogasaki.proto.SqlRequest.PlaceHolder.TypeInfoCase;

class PlaceholdersTest {
    
    @Test
    void ofKind() {
        var result = Placeholders.of("a", AtomType.INT4);
    
        assertEquals("a", result.getName());
        assertEquals(TypeInfoCase.TYPE, result.getTypeInfoCase());
        assertEquals(AtomType.INT4, result.getType());
        assertEquals(0, result.getDimension());
    }
    
    @Test
    void ofClass() {
        assertEquals(Placeholders.of("x", AtomType.CHARACTER), Placeholders.of("x", String.class));
    }
    
    @Test
    void ofInfoArray() {
        var result = Placeholders.of("x", Types.array(int.class));
        assertEquals(TypeInfoCase.TYPE, result.getTypeInfoCase());
        assertEquals(AtomType.INT4, result.getType());
        assertEquals(1, result.getDimension());
    }
    
    @Test
    void ofInfoRow() {
        var type = Types.row(Types.column("c", int.class), Types.column("d", String.class));
        var result = Placeholders.of("x", type);
        assertEquals(TypeInfoCase.ROW_TYPE, result.getTypeInfoCase());
        assertEquals(type.getRowType(), result.getRowType());
        assertEquals(0, result.getDimension());
    }
    
    @Test
    void ofInfoUser() {
        var type = Types.user("Hello");
        var result = Placeholders.of("x", type);
        assertEquals(TypeInfoCase.USER_TYPE, result.getTypeInfoCase());
        assertEquals(type.getUserType(), result.getUserType());
        assertEquals(0, result.getDimension());
    }
}
