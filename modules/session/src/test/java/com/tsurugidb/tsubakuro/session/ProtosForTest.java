package com.tsurugidb.tsubakuro.session;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;

import static org.junit.jupiter.api.Assertions.*;

// import org.junit.jupiter.api.Test;

//FIXME: delete tests for generated sources
public final class ProtosForTest {
    private ProtosForTest() {
        // for checkstyle
    }
    
    static class TransactionChecker {
        static SqlCommon.Transaction.Builder builder() {
            return
                SqlCommon.Transaction.newBuilder()
                .setHandle(456);
        }
        static boolean check(SqlCommon.Transaction dst) {
            return
                (dst.getHandle() == 456);
        }
        
        void test() {
            try {
                assertTrue(check(SqlCommon.Transaction.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class PreparedStatementChecker {
        static SqlCommon.PreparedStatement.Builder builder() {
            return
                SqlCommon.PreparedStatement.newBuilder()
                .setHandle(789);
        }
        static boolean check(SqlCommon.PreparedStatement dst) {
            return
                (dst.getHandle() == 789);
        }
        
        void test() {
            try {
                assertTrue(check(SqlCommon.PreparedStatement.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    
    /**
     * Check of Request parts
     */
    static class PlaceHolderChecker {
        static SqlRequest.Placeholder.Builder builder() {
            return
                SqlRequest.Placeholder.newBuilder()
                .setName("v1").setAtomType(SqlCommon.AtomType.INT8);
        }
        static boolean check(SqlRequest.Placeholder dst) {
            return
                dst.getName().equals("v1")
                && dst.getAtomType().equals(SqlCommon.AtomType.INT8);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Placeholder.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    static class PlaceHolderChecker2 {
        static SqlRequest.Placeholder.Builder builder() {
            return
                SqlRequest.Placeholder.newBuilder().setName("v2").setAtomType(SqlCommon.AtomType.FLOAT8);
        }
        static boolean check(SqlRequest.Placeholder dst) {
            return
                dst.getName().equals("v2")
                && dst.getAtomType().equals(SqlCommon.AtomType.FLOAT8);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Placeholder.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class ParameterSetChecker {
        static SqlRequest.Parameter.Builder builder() {
            return
                SqlRequest.Parameter.newBuilder().setName("v1").setInt4Value(11);
        }
        static boolean check(SqlRequest.Parameter dst) {
            return
                dst.getName().equals("v1")
                && SqlRequest.Parameter.ValueCase.INT4_VALUE.equals(dst.getValueCase());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Parameter.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    static class ParameterSetChecker2 {
        static SqlRequest.Parameter.Builder builder() {
            return
                SqlRequest.Parameter.newBuilder().setName("v2").setFloat8Value(123.45);
        }
        static boolean check(SqlRequest.Parameter dst) {
            return
                dst.getName().equals("v2")
                && SqlRequest.Parameter.ValueCase.FLOAT8_VALUE.equals(dst.getValueCase());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Parameter.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class TransactionOptionChecker {
        static final String TABLE_NAME = "table_for_preserve";
        static SqlRequest.TransactionOption.Builder builder() {
            return
                SqlRequest.TransactionOption.newBuilder()
                .setType(SqlRequest.TransactionType.SHORT)
                .addWritePreserves(SqlRequest.WritePreserve.newBuilder().setTableName(TABLE_NAME));
        }
        static boolean check(SqlRequest.TransactionOption dst) {
            SqlRequest.WritePreserve r1 = dst.getWritePreservesList().get(0);
            
            return
                dst.getType().equals(SqlRequest.TransactionType.SHORT)
                && r1.getTableName().equals(TABLE_NAME)
                && (dst.getWritePreservesList().size() == 1);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.TransactionOption.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    
    /**
     * Check of each Request
     */
    public static class BeginChecker {
        public static SqlRequest.Begin.Builder builder() {
            return
                SqlRequest.Begin.newBuilder()
                .setOption(TransactionOptionChecker.builder());
        }
        public static boolean check(SqlRequest.Begin dst) {
            return
                TransactionOptionChecker.check(dst.getOption());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Begin.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class PrepareChecker {
        static final String SQL = "SELECT a, b, c FROM t WHERE d = 321";
        static SqlRequest.Prepare.Builder builder() {
            return
                SqlRequest.Prepare.newBuilder()
                .setSql(SQL)
                .addPlaceholders(PlaceHolderChecker.builder())
                .addPlaceholders(PlaceHolderChecker2.builder());
        }
        static boolean check(SqlRequest.Prepare dst) {
            var p1 = dst.getPlaceholdersList().get(0);
            var p2 = dst.getPlaceholdersList().get(1);
            
            return
                dst.getSql().equals(SQL)
                && PlaceHolderChecker.check(p1)
                && PlaceHolderChecker2.check(p2);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Prepare.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class ExecuteStatementChecker {
        static final String SQL = "UPDATE t SET a = a + 1 WHERE d = 654";
        static SqlRequest.ExecuteStatement.Builder builder() {
            return
                SqlRequest.ExecuteStatement.newBuilder()
                .setTransactionHandle(TransactionChecker.builder())
                .setSql(SQL);
        }
        static boolean check(SqlRequest.ExecuteStatement dst) {
            return
                TransactionChecker.check(dst.getTransactionHandle())
                && dst.getSql().equals(SQL);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.ExecuteStatement.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class ExecuteQueryChecker {
        static final String SQL = "SELECT x, y, z FROM t WHERE d = 987";
        static SqlRequest.ExecuteQuery.Builder builder() {
            return
                SqlRequest.ExecuteQuery.newBuilder()
                .setTransactionHandle(TransactionChecker.builder())
                .setSql(SQL);
        }
        static boolean check(SqlRequest.ExecuteQuery dst) {
            return
                TransactionChecker.check(dst.getTransactionHandle())
                && dst.getSql().equals(SQL);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.ExecuteQuery.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class ExecutePreparedStatementChecker {
        static SqlRequest.ExecutePreparedStatement.Builder builder() {
            return
                SqlRequest.ExecutePreparedStatement.newBuilder()
                .setTransactionHandle(TransactionChecker.builder())
                .setPreparedStatementHandle(PreparedStatementChecker.builder())
                .addParameters(ParameterSetChecker.builder())
                .addParameters(ParameterSetChecker2.builder());
        }
        static boolean check(SqlRequest.ExecutePreparedStatement dst) {
            var p1 = dst.getParametersList().get(0);
            var p2 = dst.getParametersList().get(1);
            
            return
                TransactionChecker.check(dst.getTransactionHandle())
                && PreparedStatementChecker.check(dst.getPreparedStatementHandle())
                && ParameterSetChecker.check(p1)
                && ParameterSetChecker2.check(p2);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.ExecutePreparedStatement.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class ExecutePreparedQueryChecker {
        static SqlRequest.ExecutePreparedQuery.Builder builder() {
            return
                SqlRequest.ExecutePreparedQuery.newBuilder()
                .setTransactionHandle(TransactionChecker.builder())
                .setPreparedStatementHandle(PreparedStatementChecker.builder())
                .addParameters(ParameterSetChecker.builder())
                .addParameters(ParameterSetChecker2.builder());
        }
        static boolean check(SqlRequest.ExecutePreparedQuery dst) {
            var p1 = dst.getParametersList().get(0);
            var p2 = dst.getParametersList().get(1);
            
            return
                TransactionChecker.check(dst.getTransactionHandle())
                && PreparedStatementChecker.check(dst.getPreparedStatementHandle())
                && ParameterSetChecker.check(p1)
                && ParameterSetChecker2.check(p2);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.ExecutePreparedQuery.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class ExecuteDumpChecker {
        static final String PATH = "/This/Is/A/Path/String";
        static SqlRequest.ExecuteDump.Builder builder() {
            return
                SqlRequest.ExecuteDump.newBuilder()
                .setTransactionHandle(TransactionChecker.builder())
                .setPreparedStatementHandle(PreparedStatementChecker.builder())
                .addParameters(ParameterSetChecker.builder())
                .addParameters(ParameterSetChecker2.builder())
                .setDirectory(PATH);
        }
        static boolean check(SqlRequest.ExecuteDump dst) {
            var p1 = dst.getParametersList().get(0);
            var p2 = dst.getParametersList().get(1);
            
            return
                TransactionChecker.check(dst.getTransactionHandle())
                && PreparedStatementChecker.check(dst.getPreparedStatementHandle())
                && ParameterSetChecker.check(p1)
                && ParameterSetChecker2.check(p2)
                && dst.getDirectory().equals(PATH);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.ExecuteDump.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class ExecuteLoadChecker {
        static final String PATH = "/This/Is/A/Path/String";
        static SqlRequest.ExecuteLoad.Builder builder() {
            return
                SqlRequest.ExecuteLoad.newBuilder()
                .setTransactionHandle(TransactionChecker.builder())
                .setPreparedStatementHandle(PreparedStatementChecker.builder())
                .addParameters(ParameterSetChecker.builder())
                .addParameters(ParameterSetChecker2.builder())
                .addFile(PATH);
        }
        static boolean check(SqlRequest.ExecuteLoad dst) {
            var p1 = dst.getParametersList().get(0);
            var p2 = dst.getParametersList().get(1);
            
            return
                TransactionChecker.check(dst.getTransactionHandle())
                && PreparedStatementChecker.check(dst.getPreparedStatementHandle())
                && ParameterSetChecker.check(p1)
                && ParameterSetChecker2.check(p2)
                && dst.getFileList().get(0).equals(PATH);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.ExecuteLoad.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class CommitChecker {
        static SqlRequest.Commit.Builder builder() {
            return
                SqlRequest.Commit.newBuilder()
                .setTransactionHandle(TransactionChecker.builder());
        }
        static boolean check(SqlRequest.Commit dst) {
            return
                TransactionChecker.check(dst.getTransactionHandle());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Commit.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class RollbackChecker {
        static SqlRequest.Rollback.Builder builder() {
            return
                SqlRequest.Rollback.newBuilder()
                .setTransactionHandle(TransactionChecker.builder());
        }
        static boolean check(SqlRequest.Rollback dst) {
            return
                TransactionChecker.check(dst.getTransactionHandle());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Rollback.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static class DisposePreparedStatementChecker {
        static SqlRequest.DisposePreparedStatement.Builder builder() {
            return
                SqlRequest.DisposePreparedStatement.newBuilder()
                .setPreparedStatementHandle(PreparedStatementChecker.builder());
        }
        static boolean check(SqlRequest.DisposePreparedStatement dst) {
            return
                PreparedStatementChecker.check(dst.getPreparedStatementHandle());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.DisposePreparedStatement.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExplainChecker {
        public static SqlRequest.Explain.Builder builder() {
            return
                SqlRequest.Explain.newBuilder()
                .setPreparedStatementHandle(PreparedStatementChecker.builder())
                .addParameters(ParameterSetChecker.builder())
                .addParameters(ParameterSetChecker2.builder());
        }
        static boolean check(SqlRequest.Explain dst) {
            var p1 = dst.getParametersList().get(0);
            var p2 = dst.getParametersList().get(1);
            
            return
                PreparedStatementChecker.check(dst.getPreparedStatementHandle())
                && ParameterSetChecker.check(p1)
                && ParameterSetChecker2.check(p2);
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Explain.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    static long sessionID = 123;
    
    /**
     * Check of Request level message
     * can be used by external packages
     */
    public static class BeginRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setBegin(BeginChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setBegin(BeginChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                SqlRequest.Request.RequestCase.BEGIN.equals(dst.getRequestCase())
                && BeginChecker.check(dst.getBegin());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class PrepareRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setPrepare(PrepareChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {
            return
                SqlRequest.Request.newBuilder()
                .setPrepare(PrepareChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                (dst.getSessionHandle().getHandle() == id)
                && SqlRequest.Request.RequestCase.PREPARE.equals(dst.getRequestCase())
                && PrepareChecker.check(dst.getPrepare());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExecuteStatementRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setExecuteStatement(ExecuteStatementChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setExecuteStatement(ExecuteStatementChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                SqlRequest.Request.RequestCase.EXECUTE_STATEMENT.equals(dst.getRequestCase())
                && ExecuteStatementChecker.check(dst.getExecuteStatement());
        }

        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExecuteQueryRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setExecuteQuery(ExecuteQueryChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setExecuteQuery(ExecuteQueryChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                SqlRequest.Request.RequestCase.EXECUTE_QUERY.equals(dst.getRequestCase())
                && ExecuteQueryChecker.check(dst.getExecuteQuery());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExecutePreparedStatementRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setExecutePreparedStatement(ExecutePreparedStatementChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setExecutePreparedStatement(ExecutePreparedStatementChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                (dst.getSessionHandle().getHandle() == id)
                && SqlRequest.Request.RequestCase.EXECUTE_PREPARED_STATEMENT.equals(dst.getRequestCase())
                && ExecutePreparedStatementChecker.check(dst.getExecutePreparedStatement());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExecutePreparedQueryRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setExecutePreparedQuery(ExecutePreparedQueryChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setExecutePreparedQuery(ExecutePreparedQueryChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                (dst.getSessionHandle().getHandle() == id)
                && SqlRequest.Request.RequestCase.EXECUTE_PREPARED_QUERY.equals(dst.getRequestCase())
                && ExecutePreparedQueryChecker.check(dst.getExecutePreparedQuery());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class CommitRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setCommit(CommitChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setCommit(CommitChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                (dst.getSessionHandle().getHandle() == id)
                && SqlRequest.Request.RequestCase.COMMIT.equals(dst.getRequestCase())
                && CommitChecker.check(dst.getCommit());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class RollbackRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setRollback(RollbackChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setRollback(RollbackChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                (dst.getSessionHandle().getHandle() == id)
                && SqlRequest.Request.RequestCase.ROLLBACK.equals(dst.getRequestCase())
                && RollbackChecker.check(dst.getRollback());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class DisposePreparedStatementRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setDisposePreparedStatement(DisposePreparedStatementChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setDisposePreparedStatement(DisposePreparedStatementChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                (dst.getSessionHandle().getHandle() == id)
                && SqlRequest.Request.RequestCase.DISPOSE_PREPARED_STATEMENT.equals(dst.getRequestCase())
                && DisposePreparedStatementChecker.check(dst.getDisposePreparedStatement());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExplainRequestChecker {
        static SqlRequest.Request.Builder builder(long id) {
            return
                SqlRequest.Request.newBuilder()
                .setSessionHandle(SqlCommon.Session.newBuilder().setHandle(id))
                .setExplain(ExplainChecker.builder());
        }
        public static SqlRequest.Request.Builder builder() {  // SessionHandle won't be set
            return
                SqlRequest.Request.newBuilder()
                .setExplain(ExplainChecker.builder());
        }
        public static boolean check(SqlRequest.Request dst, long id) {
            return
                (dst.getSessionHandle().getHandle() == id)
                && SqlRequest.Request.RequestCase.EXPLAIN.equals(dst.getRequestCase())
                && ExplainChecker.check(dst.getExplain());
        }
        
        void test() {
            try {
                assertTrue(check(SqlRequest.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    
    /**
     * Check of Response parts
     */
    static class SuccessChecker {
        static SqlResponse.Success.Builder builder() {
            return
                SqlResponse.Success.newBuilder();
        }
        static boolean check(SqlResponse.Success dst) {
            return
                true;
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Success.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    static class ErrorChecker {
        static final String ERROR = "This is a error for test purpose";
        static SqlResponse.Error.Builder builder() {
            return
                SqlResponse.Error.newBuilder()
                .setDetail(ERROR);
        }
        static boolean check(SqlResponse.Error dst) {
            return
                dst.getDetail().equals(ERROR);
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Error.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    /**
     * Check of each Response
     */
    public static class ResultOnlyChecker {
        static SqlResponse.ResultOnly.Builder builder() {
            return
                SqlResponse.ResultOnly.newBuilder()
                .setSuccess(SuccessChecker.builder());
        }
        public static boolean check(SqlResponse.ResultOnly dst) {
            return
                SqlResponse.ResultOnly.ResultCase.SUCCESS.equals(dst.getResultCase())
                && SuccessChecker.check(dst.getSuccess());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.ResultOnly.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ResMessageBeginChecker {
        static SqlResponse.Begin.Builder builder() {
            return
                SqlResponse.Begin.newBuilder()
                .setSuccess(SqlResponse.Begin.Success.newBuilder().setTransactionHandle(TransactionChecker.builder()));
        }
        public static boolean check(SqlResponse.Begin dst) {
            return
                SqlResponse.Begin.ResultCase.SUCCESS.equals(dst.getResultCase())
                && TransactionChecker.check(dst.getSuccess().getTransactionHandle());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Begin.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ResMessagePrepareChecker {
        static SqlResponse.Prepare.Builder builder() {
            return
                SqlResponse.Prepare.newBuilder()
                .setPreparedStatementHandle(PreparedStatementChecker.builder());
        }
        public static boolean check(SqlResponse.Prepare dst) {
            return
                SqlResponse.Prepare.ResultCase.PREPARED_STATEMENT_HANDLE.equals(dst.getResultCase())
                && PreparedStatementChecker.check(dst.getPreparedStatementHandle());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Prepare.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ResMessageExecuteQueryChecker {
        static final String RESYKTSET_NAME = "ResultSetName";
        static SqlResponse.ExecuteQuery.Builder builder() {
            return
                SqlResponse.ExecuteQuery.newBuilder()
                .setName(RESYKTSET_NAME)
                .setRecordMeta(SchemaProtosChecker.builder());
        }
        public static boolean check(SqlResponse.ExecuteQuery dst) {
            return
                dst.getName().equals(RESYKTSET_NAME)
                && SchemaProtosChecker.check(dst.getRecordMeta());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.ExecuteQuery.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ResMessageExplainChecker {
        static final String EXPLAIN = "ThisIsAnExecutionPlanString";
        public static SqlResponse.Explain.Builder builder() {
            return
                SqlResponse.Explain.newBuilder()
                .setOutput(EXPLAIN);
        }
        public static boolean check(SqlResponse.Explain dst) {
            return
                dst.getOutput().equals(EXPLAIN);
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Explain.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    /**
     * Check of Response level message
     * can be used by external packages
     */
    public static class ResultOnlyResponseChecker {
        public static SqlResponse.Response.Builder builder() {
            return
                SqlResponse.Response.newBuilder()
                .setResultOnly(ResultOnlyChecker.builder());
        }
        public static boolean check(SqlResponse.Response dst) {
            return
                SqlResponse.Response.ResponseCase.RESULT_ONLY.equals(dst.getResponseCase())
                && ResultOnlyChecker.check(dst.getResultOnly());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Response.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class BeginResponseChecker {
        public static SqlResponse.Response.Builder builder() {
            return
                SqlResponse.Response.newBuilder()
                .setBegin(ResMessageBeginChecker.builder());
        }
        public static boolean check(SqlResponse.Response dst) {
            return
                SqlResponse.Response.ResponseCase.BEGIN.equals(dst.getResponseCase())
                && ResMessageBeginChecker.check(dst.getBegin());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Response.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class PrepareResponseChecker {
        public static SqlResponse.Response.Builder builder() {
            return
                SqlResponse.Response.newBuilder()
                .setPrepare(ResMessagePrepareChecker.builder());
        }
        public static boolean check(SqlResponse.Response dst) {
            return
                SqlResponse.Response.ResponseCase.PREPARE.equals(dst.getResponseCase())
                && ResMessagePrepareChecker.check(dst.getPrepare());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Response.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExecuteQueryResponseChecker {
        public static SqlResponse.Response.Builder builder() {
            return
                SqlResponse.Response.newBuilder()
                .setExecuteQuery(ResMessageExecuteQueryChecker.builder());
        }
        public static boolean check(SqlResponse.Response dst) {
            return
                SqlResponse.Response.ResponseCase.EXECUTE_QUERY.equals(dst.getResponseCase())
                && ResMessageExecuteQueryChecker.check(dst.getExecuteQuery());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Response.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    public static class ExplainResponseChecker {
        public static SqlResponse.Response.Builder builder() {
            return
                SqlResponse.Response.newBuilder()
                .setExplain(ResMessageExplainChecker.builder());
        }
        public static boolean check(SqlResponse.Response dst) {
            return
                SqlResponse.Response.ResponseCase.EXPLAIN.equals(dst.getResponseCase())
                && ResMessageExplainChecker.check(dst.getExplain());
        }
        
        void test() {
            try {
                assertTrue(check(SqlResponse.Response.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
    
    
    /**
     * Check of Schema meta data
     */
    public static class SchemaProtosChecker {
        public static SqlResponse.ResultSetMetadata.Builder builder() {
            return
                SqlResponse.ResultSetMetadata.newBuilder()
                .addColumns(SqlCommon.Column.newBuilder().setName("v1").setAtomType(SqlCommon.AtomType.INT8))
                .addColumns(SqlCommon.Column.newBuilder().setName("v2").setAtomType(SqlCommon.AtomType.FLOAT8))
                .addColumns(SqlCommon.Column.newBuilder().setName("v3").setAtomType(SqlCommon.AtomType.CHARACTER))
                .addColumns(SqlCommon.Column.newBuilder().setAtomType(SqlCommon.AtomType.INT8))
                .addColumns(SqlCommon.Column.newBuilder().setAtomType(SqlCommon.AtomType.FLOAT8))
                .addColumns(SqlCommon.Column.newBuilder().setAtomType(SqlCommon.AtomType.CHARACTER));
        }
        public static boolean check(SqlResponse.ResultSetMetadata dst) {
            SqlCommon.Column v1 = dst.getColumnsList().get(0);
            SqlCommon.Column v2 = dst.getColumnsList().get(1);
            SqlCommon.Column v3 = dst.getColumnsList().get(2);
            SqlCommon.Column v4 = dst.getColumnsList().get(3);
            SqlCommon.Column v5 = dst.getColumnsList().get(4);
            SqlCommon.Column v6 = dst.getColumnsList().get(5);
            
            return
                v1.getName().equals("v1")
                && v1.getAtomType().equals(SqlCommon.AtomType.INT8)
                && v2.getName().equals("v2")
                && v2.getAtomType().equals(SqlCommon.AtomType.FLOAT8)
                && v3.getName().equals("v3")
                && v3.getAtomType().equals(SqlCommon.AtomType.CHARACTER)
                && v4.getAtomType().equals(SqlCommon.AtomType.INT8)
                && v4.getName().equals("")
                && v5.getName().equals("")
                && v5.getAtomType().equals(SqlCommon.AtomType.FLOAT8)
                && v6.getName().equals("")
                && v6.getAtomType().equals(SqlCommon.AtomType.CHARACTER)
                && (dst.getColumnsList().size() == 6);
        }

        void test() {
            try {
                assertTrue(check(SqlResponse.ResultSetMetadata.parseFrom(builder().build().toByteArray())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                fail("cought com.google.protobuf.InvalidProtocolBufferException");
            }
        }
    }
}
