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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.sql.Types;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.TypeInfo;

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
