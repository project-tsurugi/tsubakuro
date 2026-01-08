package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.test.util.DbTester;

class DbSqlExplainTest extends DbTester {

    private static final int ATTEMPT_SIZE = 100;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSqlExplainTest.class);
        logInitStart(LOG, info);

        dropTableIfExists("test");
        var sql = "create table test (\n" //
                + "  pk int primary key,\n" //
                + "  long_value bigint,\n" //
                + "  string_value varchar(10)\n" //
                + ")";
        executeDdl(sql);

        logInitEnd(LOG, info);
    }

    @Test
    void explain() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            var sql = "select * from test";
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                StatementMetadata metadata = sqlClient.explain(sql).await(10, TimeUnit.SECONDS);

                assertStatementMetadata(metadata);
            }
        }
    }

    @Test
    void explainParameterList() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            var sql = "select * from test where pk = :pk";
            var placeholders = List.of(Placeholders.of("pk", AtomType.INT4));
            try (var ps = sqlClient.prepare(sql, placeholders).await(10, TimeUnit.SECONDS)) {
                for (int i = 0; i < ATTEMPT_SIZE; i++) {
                    var parameter = List.of(Parameters.of("pk", 123));
                    StatementMetadata metadata = sqlClient.explain(ps, parameter).await(10, TimeUnit.SECONDS);

                    assertStatementMetadata(metadata);
                }
            }
        }
    }

    @Test
    void explainParameters() throws Exception {
        try (var sqlClient = SqlClient.attach(getSession())) {
            var sql = "select * from test where pk = :pk";
            try (var ps = sqlClient.prepare(sql, Placeholders.of("pk", AtomType.INT4)).await(10, TimeUnit.SECONDS)) {
                for (int i = 0; i < ATTEMPT_SIZE; i++) {
                    var parameter = Parameters.of("pk", 123);
                    StatementMetadata metadata = sqlClient.explain(ps, parameter).await(10, TimeUnit.SECONDS);

                    assertStatementMetadata(metadata);
                }
            }
        }
    }

    private static void assertStatementMetadata(StatementMetadata metadata) {
        assertNotNull(metadata.getContents());
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
}
