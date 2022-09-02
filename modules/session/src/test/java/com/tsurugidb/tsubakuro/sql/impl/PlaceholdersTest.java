package com.tsurugidb.tsubakuro.sql.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder.TypeInfoCase;

class PlaceholdersTest {
    
    @Test
    void ofKind() {
        var result = Placeholders.of("a", AtomType.INT4);
    
        assertEquals("a", result.getName());
        assertEquals(TypeInfoCase.ATOM_TYPE, result.getTypeInfoCase());
        assertEquals(AtomType.INT4, result.getAtomType());
        assertEquals(0, result.getDimension());
    }
    
    @Test
    void ofClass() {
        assertEquals(Placeholders.of("x", AtomType.CHARACTER), Placeholders.of("x", String.class));
    }
    
    @Test
    void ofInfoArray() {
        var result = Placeholders.of("x", Types.array(int.class));
        assertEquals(TypeInfoCase.ATOM_TYPE, result.getTypeInfoCase());
        assertEquals(AtomType.INT4, result.getAtomType());
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
