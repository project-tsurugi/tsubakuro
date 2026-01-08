package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.sql.SearchPath;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.exception.TargetNotFoundException;
import com.tsurugidb.tsubakuro.test.util.DbTester;

class DbSqlClientTest extends DbTester {

    private static final int ATTEMPT_SIZE = 100;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSqlClientTest.class);
        logInitStart(LOG, info);

        dropTableIfExists("test");
        var sql = "create table test (\n" //
                + "  pk int primary key,\n" //
                + "  long_value bigint,\n" //
                + "  string_value varchar(10)\n" //
                + ")";
        executeDdl(sql);

        dropTableIfExists("test2");

        logInitEnd(LOG, info);
    }

    @Test
    void getTableMetadata() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                TableMetadata metadata = sqlClient.getTableMetadata("test").await(10, TimeUnit.SECONDS);
                assertTableMetadata(metadata);

                var e = assertThrowsExactly(TargetNotFoundException.class, () -> {
                    sqlClient.getTableMetadata("test2").await(10, TimeUnit.SECONDS);
                });
                assertEquals(SqlServiceCode.TARGET_NOT_FOUND_EXCEPTION, e.getDiagnosticCode());
                assertTrue(e.getMessage().contains("test2"));
            }
        }
    }

    private static void assertTableMetadata(TableMetadata metadata) {
        assertEquals(Optional.empty(), metadata.getDatabaseName());
        assertEquals(Optional.empty(), metadata.getSchemaName());
        assertEquals("test", metadata.getTableName());
        assertColumns(metadata.getColumns());
    }

    private static void assertColumns(List<? extends Column> columnList) {
        assertEquals(3, columnList.size());
        int i = 0;
        {
            var column = columnList.get(i++);
            assertEquals("pk", column.getName());
            assertEquals(AtomType.INT4, column.getAtomType());
        }
        {
            var column = columnList.get(i++);
            assertEquals("long_value", column.getName());
            assertEquals(AtomType.INT8, column.getAtomType());
        }
        {
            var column = columnList.get(i++);
            assertEquals("string_value", column.getName());
            assertEquals(AtomType.CHARACTER, column.getAtomType());
        }
    }

    @Test
    void listTables() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                TableList tableList = sqlClient.listTables().await(10, TimeUnit.SECONDS);
                {
                    List<String> list = tableList.getTableNames();
                    assertTrue(list.contains("test"));
                    assertFalse(list.contains("test2"));
                    assertFalse(list.stream().anyMatch(name -> name.startsWith("__")));
                }
                {
                    // TODO tableList.getSimpleNames() test
                }
            }
        }
    }

    @Test
    void getSearchPath() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                SearchPath searchPath = sqlClient.getSearchPath().await(10, TimeUnit.SECONDS);
                {
                    List<String> list = searchPath.getSchemaNames();
                    assertTrue(list.isEmpty()); // TODO searchPath.getSchemaNames() test
                }
            }
        }
    }
}
