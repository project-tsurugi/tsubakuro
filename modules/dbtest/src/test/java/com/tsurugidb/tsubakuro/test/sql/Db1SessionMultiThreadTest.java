package com.tsurugidb.tsubakuro.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.CounterType;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.test.util.DbTester;

/**
 * 1 session multi-thread test.
 */
class Db1SessionMultiThreadTest extends DbTester {

    private static final int DATA_SIZE = 200;
    private static final int ATTEMPT_SIZE = 100;

    @Test
    void test1SsessionMultiThread() throws Exception {
        execute(2, 2, 4);
    }

    private void execute(int insertTaskSize, int updateTaskSize, int selectTaskSize) throws Exception {
        var client = SqlClient.attach(getSession());

        int columnSize = 20;

        var taskList = new ArrayList<CheckTask>();
        for (int i = 0; i < insertTaskSize; i++) {
            var tableName = "i" + i;
            taskList.add(new InsertTask(client, tableName, columnSize));
        }
        for (int i = 0; i < updateTaskSize; i++) {
            var tableName = "u" + i;
            taskList.add(new UpdateTask(client, tableName, columnSize));
        }
        for (int i = 0; i < selectTaskSize; i++) {
            var tableName = "s" + i;
            taskList.add(new SelectTask(client, tableName, columnSize));
        }

        for (var task : taskList) {
            task.initialize();
        }

        var service = Executors.newCachedThreadPool();
        try {
            var futureList = service.invokeAll(taskList);
            for (var future : futureList) {
                future.get();
            }
        } finally {
            service.shutdownNow();
        }

        for (var task : taskList) {
            task.check();
        }
    }

    private static abstract class CheckTask implements Callable<Void> {
        protected final SqlClient client;
        protected final String tableName;
        protected final int columnSize;

        public CheckTask(SqlClient client, String tableName, int columnSize) {
            this.client = client;
            this.tableName = tableName;
            this.columnSize = columnSize;
        }

        public abstract void initialize() throws Exception;

        protected final void createTable() throws IOException, ServerException, InterruptedException, TimeoutException {
            dropTableIfExists(tableName);

            var sb = new StringBuilder();
            sb.append("create table ");
            sb.append(tableName);
            sb.append("(");
            sb.append("pk int,");
            for (int i = 0; i < columnSize; i++) {
                sb.append("value");
                sb.append(i);
                sb.append(" bigint,");
            }
            sb.append("primary key(pk)");
            sb.append(")");
            executeDdl(sb.toString());
        }

        protected final void insert(int size) throws ServerException, IOException, InterruptedException, TimeoutException {
            try (var ps = createInsertStatement()) {
                executeOcc(tx -> {
                    for (int i = 0; i < size; i++) {
                        var parameter = createInsertParameter(i);
                        var result = tx.executeStatement(ps, parameter).await();
                        assertEquals(1L, result.getCounters().get(CounterType.INSERTED_ROWS));
                    }
                });
            }
        }

        protected final PreparedStatement createInsertStatement() throws IOException, ServerException, InterruptedException {
            var sb = new StringBuffer();
            var placeholders = new ArrayList<Placeholder>();

            sb.append("insert into ");
            sb.append(tableName);
            sb.append(" values(:pk");
            placeholders.add(Placeholders.of("pk", AtomType.INT4));
            for (int i = 0; i < columnSize; i++) {
                String value = "value" + i;

                sb.append(", :");
                sb.append(value);

                placeholders.add(Placeholders.of(value, AtomType.INT8));
            }
            sb.append(")");
            var sql = sb.toString();

            var ps = client.prepare(sql, placeholders).await();
            return ps;
        }

        protected final List<Parameter> createInsertParameter(int pk) {
            var parameter = new ArrayList<Parameter>();

            parameter.add(Parameters.of("pk", pk));
            for (int j = 0; j < columnSize; j++) {
                parameter.add(Parameters.of("value" + j, pk + j + 1L));
            }

            return parameter;
        }

        @Override
        public final Void call() throws Exception {
            execute();
            return null;
        }

        protected abstract void execute() throws Exception;

        public abstract void check() throws Exception;
    }

    private static class InsertTask extends CheckTask {
        private final int insertSize;

        InsertTask(SqlClient client, String tableName, int columnSize) {
            super(client, tableName, columnSize);
            this.insertSize = ATTEMPT_SIZE;
        }

        @Override
        public void initialize() throws Exception {
            createTable();
        }

        @Override
        protected void execute() throws Exception {
            try (var ps = createInsertStatement(); //
                    var tx = client.createTransaction().await()) {
                for (int i = 0; i < insertSize; i++) {
                    var parameter = createInsertParameter(i);

                    var result = tx.executeStatement(ps, parameter).await();
                    assertEquals(1L, result.getCounters().get(CounterType.INSERTED_ROWS));
                }

                tx.commit().await();
            }
        }

        @Override
        public void check() throws ServerException, IOException, InterruptedException {
            try (var tx = client.createTransaction().await(); //
                    var rs = tx.executeQuery("select * from " + tableName + " order by pk").await()) {
                int count = 0;
                while (rs.nextRow()) {
                    int expectedPk = count++;

                    assertTrue(rs.nextColumn());
                    int pk = rs.fetchInt4Value();
                    assertEquals(expectedPk, pk);

                    for (int i = 0; i < columnSize; i++) {
                        assertTrue(rs.nextColumn());
                        long value = rs.fetchInt8Value();
                        assertEquals(expectedPk + i + 1L, value);
                    }
                    assertFalse(rs.nextColumn());
                }
                assertEquals(insertSize, count);

                tx.commit().await();
            }
        }
    }

    private static class UpdateTask extends CheckTask {
        private final int insertSize;
        private final int updateCount;

        UpdateTask(SqlClient client, String tableName, int columnSize) {
            super(client, tableName, columnSize);
            this.insertSize = DATA_SIZE;
            this.updateCount = ATTEMPT_SIZE;
        }

        @Override
        public void initialize() throws Exception {
            createTable();
            insert(insertSize);
        }

        @Override
        protected void execute() throws Exception {
            var sql = "update " + tableName + " set " //
                    + "value0 = case when value1 > value2 then 0 else 1 end," //
                    + "value1 = value2, " //
                    + "value2 = value1, " //
                    + "value3 = :count";
            var plaeholders = List.of(Placeholders.of("count", AtomType.INT8));

            try (var ps = client.prepare(sql, plaeholders).await()) {
                for (int i = 0; i < updateCount; i++) {
                    try (var tx = client.createTransaction().await()) {
                        var parameter = List.of(Parameters.of("count", (long) i));

                        var result = tx.executeStatement(ps, parameter).await();
                        assertEquals(insertSize, result.getCounters().get(CounterType.UPDATED_ROWS));

                        tx.commit().await();
                    }
                }
            }
        }

        @Override
        public void check() throws ServerException, IOException, InterruptedException {
            try (var tx = client.createTransaction().await(); //
                    var rs = tx.executeQuery("select * from " + tableName + " order by pk").await()) {
                int count = 0;
                while (rs.nextRow()) {
                    int expectedPk = count++;

                    assertTrue(rs.nextColumn());
                    int pk = rs.fetchInt4Value();
                    assertEquals(expectedPk, pk);

                    assertTrue(rs.nextColumn());
                    long value0 = rs.fetchInt8Value();
                    assertTrue(rs.nextColumn());
                    long value1 = rs.fetchInt8Value();
                    assertTrue(rs.nextColumn());
                    long value2 = rs.fetchInt8Value();
                    if (value0 == 0) {
                        assertEquals(pk + 2, value1);
                        assertEquals(pk + 3, value2);
                    } else if (value0 == 1) {
                        assertEquals(pk + 2, value2);
                        assertEquals(pk + 3, value1);
                    } else {
                        fail("value0=" + value0);
                    }

                    assertTrue(rs.nextColumn());
                    long value3 = rs.fetchInt8Value();
                    assertEquals(updateCount - 1, value3);
                }
                assertEquals(insertSize, count);

                tx.commit().await();
            }
        }
    }

    private static class SelectTask extends CheckTask {
        private final int insertSize;
        private final int selectCount;

        SelectTask(SqlClient client, String tableName, int columnSize) {
            super(client, tableName, columnSize);
            this.insertSize = DATA_SIZE;
            this.selectCount = ATTEMPT_SIZE;
        }

        @Override
        public void initialize() throws Exception {
            createTable();
            insert(insertSize);
        }

        @Override
        protected void execute() throws Exception {
            var sql = "select * from " + tableName + " order by pk";

            try (var ps = client.prepare(sql, List.of()).await()) {
                for (int i = 0; i < selectCount; i++) {
                    try (var tx = client.createTransaction().await()) {
                        try (var rs = tx.executeQuery(ps, List.of()).await()) {
                            int count = 0;
                            while (rs.nextRow()) {
                                int expectedPk = count++;

                                assertTrue(rs.nextColumn());
                                int pk = rs.fetchInt4Value();
                                assertEquals(expectedPk, pk);

                                for (int j = 0; j < columnSize; j++) {
                                    assertTrue(rs.nextColumn());
                                    long value = rs.fetchInt8Value();
                                    assertEquals(expectedPk + j + 1L, value);
                                }
                                assertFalse(rs.nextColumn());
                            }
                            assertEquals(insertSize, count);
                        }

                        tx.commit().await();
                    }
                }
            }
        }

        @Override
        public void check() throws ServerException, IOException, InterruptedException {
            // do nothing
        }
    }
}
