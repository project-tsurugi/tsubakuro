package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.low.sql.Types;
import com.tsurugidb.tateyama.proto.SqlCommon.AtomType;
import com.tsurugidb.tateyama.proto.SqlCommon.TypeInfo;

class TypesTest {
        
    @Test
    void typeOf() {
        var type = Types.of(BigDecimal.class);
        assertEquals(AtomType.DECIMAL, type.getAtomType());
        assertEquals(0, type.getDimension());
    }
    
    @Test
    void typeOfArray() {
        var type = Types.of(int[].class);
        assertEquals(AtomType.INT4, type.getAtomType());
        assertEquals(1, type.getDimension());
    }
    
    @Test
    void typeOfMissing() {
        assertThrows(IllegalArgumentException.class, () -> Types.of(Object.class));
    }
    
    @Test
    void array() {
        assertEquals(Types.of(int[].class), Types.array(int.class));
    }
    
    @Test
    void arrayKind() {
        assertEquals(Types.of(String[].class), Types.array(AtomType.CHARACTER));
    }
    
    @Test
    void arrayDimensions() {
        assertEquals(Types.of(int[][][].class), Types.array(int.class, 3));
    }
    
    @Test
    void column() {
        var column = Types.column("a", int.class);
        assertEquals("a", column.getName());
        assertEquals(AtomType.INT4, column.getAtomType());
    }
    
    @Test
    void columnNoname() {
        var column = Types.column(String.class);
        assertEquals("", column.getName());
        assertEquals(AtomType.CHARACTER, column.getAtomType());
    }
    
    @Test
    void row() {
        var row = Types.row(
                            Types.column("a", int.class),
                            Types.column(String.class),
                            Types.column("b", BigDecimal.class));
        assertEquals(TypeInfo.TypeInfoCase.ROW_TYPE, row.getTypeInfoCase());
        
        var info = row.getRowType();
        assertEquals(Types.column("a", int.class), info.getColumns(0));
        assertEquals(Types.column(String.class), info.getColumns(1));
        assertEquals(Types.column("b", BigDecimal.class), info.getColumns(2));
    }
    
    @Test
    void user() {
        var user = Types.user("hello");
        assertEquals(TypeInfo.TypeInfoCase.USER_TYPE, user.getTypeInfoCase());
        assertEquals("hello", user.getUserType().getName());
    }
    
}
