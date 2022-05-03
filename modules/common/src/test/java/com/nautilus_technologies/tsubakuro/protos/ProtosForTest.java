package com.nautilus_technologies.tsubakuro.protos;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

//FIXME: delete tests for generated sources
public final class ProtosForTest {
    private ProtosForTest() {
	// for checkstyle
    }

    static class TransactionChecker {
	static CommonProtos.Transaction.Builder builder() {
	    return
		CommonProtos.Transaction.newBuilder()
		.setHandle(456);
	}
	static boolean check(CommonProtos.Transaction dst) {
	    return
		(dst.getHandle() == 456);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(CommonProtos.Transaction.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class PreparedStatementChecker {
	static CommonProtos.PreparedStatement.Builder builder() {
	    return
		CommonProtos.PreparedStatement.newBuilder()
		.setHandle(789);
	}
	static boolean check(CommonProtos.PreparedStatement dst) {
	    return
		(dst.getHandle() == 789);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(CommonProtos.PreparedStatement.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }


    /**
     * Check of Request parts
     */
    static class PlaceHolderChecker {
	static RequestProtos.PlaceHolder.Builder builder() {
	    return
		RequestProtos.PlaceHolder.newBuilder()
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("v1").setType(CommonProtos.DataType.INT8))
		.addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("v2").setType(CommonProtos.DataType.FLOAT8));
	}
	static boolean check(RequestProtos.PlaceHolder dst) {
	    RequestProtos.PlaceHolder.Variable v1 = dst.getVariablesList().get(0);
	    RequestProtos.PlaceHolder.Variable v2 = dst.getVariablesList().get(1);

	    return
		v1.getName().equals("v1")
		&& v1.getType().equals(CommonProtos.DataType.INT8)
		&& v2.getName().equals("v2")
		&& v2.getType().equals(CommonProtos.DataType.FLOAT8)
		&& (dst.getVariablesList().size() == 2);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.PlaceHolder.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class ParameterSetChecker {
	static RequestProtos.ParameterSet.Builder builder() {
	    return
		RequestProtos.ParameterSet.newBuilder()
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("v1").setInt4Value(11))
		.addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("v2").setFloat8Value(123.45));
	}
	static boolean check(RequestProtos.ParameterSet dst) {
	    RequestProtos.ParameterSet.Parameter v1 = dst.getParametersList().get(0);
	    RequestProtos.ParameterSet.Parameter v2 = dst.getParametersList().get(1);

	    return
		v1.getName().equals("v1")
		&& RequestProtos.ParameterSet.Parameter.ValueCase.INT4_VALUE.equals(v1.getValueCase())
		&& v2.getName().equals("v2")
		&& RequestProtos.ParameterSet.Parameter.ValueCase.FLOAT8_VALUE.equals(v2.getValueCase())
		&& (dst.getParametersList().size() == 2);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.ParameterSet.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class TransactionOptionChecker {
	static final String TABLES = "table_for_preserve";
	static RequestProtos.TransactionOption.Builder builder() {
	    return
		RequestProtos.TransactionOption.newBuilder()
		.setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_SHORT)
		.addWritePreserves(RequestProtos.TransactionOption.WritePreserve.newBuilder().setName(TABLES));
	}
	static boolean check(RequestProtos.TransactionOption dst) {
	    RequestProtos.TransactionOption.WritePreserve r1 = dst.getWritePreservesList().get(0);

	    return
		dst.getType().equals(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_SHORT)
		&& r1.getName().equals(TABLES)
		&& (dst.getWritePreservesList().size() == 1);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.TransactionOption.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }


    /**
     * Check of each Request
     */
    public static class BeginChecker {
	public static RequestProtos.Begin.Builder builder() {
	    return
		RequestProtos.Begin.newBuilder()
		.setOption(TransactionOptionChecker.builder());
	}
	public static boolean check(RequestProtos.Begin dst) {
	    return
		TransactionOptionChecker.check(dst.getOption());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Begin.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class PrepareChecker {
	static final String SQL = "SELECT a, b, c FROM t WHERE d = 321";
	static RequestProtos.Prepare.Builder builder() {
	    return
		RequestProtos.Prepare.newBuilder()
		.setSql(SQL)
		.setHostVariables(PlaceHolderChecker.builder());
	}
	static boolean check(RequestProtos.Prepare dst) {
	    return
		dst.getSql().equals(SQL)
		&& PlaceHolderChecker.check(dst.getHostVariables());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Prepare.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class ExecuteStatementChecker {
	static final String SQL = "UPDATE t SET a = a + 1 WHERE d = 654";
	static RequestProtos.ExecuteStatement.Builder builder() {
	    return
		RequestProtos.ExecuteStatement.newBuilder()
		.setTransactionHandle(TransactionChecker.builder())
		.setSql(SQL);
	}
	static boolean check(RequestProtos.ExecuteStatement dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle())
		&& dst.getSql().equals(SQL);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.ExecuteStatement.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class ExecuteQueryChecker {
	static final String SQL = "SELECT x, y, z FROM t WHERE d = 987";
	static RequestProtos.ExecuteQuery.Builder builder() {
	    return
		RequestProtos.ExecuteQuery.newBuilder()
		.setTransactionHandle(TransactionChecker.builder())
		.setSql(SQL);
	}
	static boolean check(RequestProtos.ExecuteQuery dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle())
		&& dst.getSql().equals(SQL);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.ExecuteQuery.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class ExecutePreparedStatementChecker {
	static RequestProtos.ExecutePreparedStatement.Builder builder() {
	    return
		RequestProtos.ExecutePreparedStatement.newBuilder()
		.setTransactionHandle(TransactionChecker.builder())
		.setPreparedStatementHandle(PreparedStatementChecker.builder())
		.setParameters(ParameterSetChecker.builder());
	}
	static boolean check(RequestProtos.ExecutePreparedStatement dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle())
		&& PreparedStatementChecker.check(dst.getPreparedStatementHandle())
		&& ParameterSetChecker.check(dst.getParameters());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.ExecutePreparedStatement.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class ExecutePreparedQueryChecker {
	static RequestProtos.ExecutePreparedQuery.Builder builder() {
	    return
		RequestProtos.ExecutePreparedQuery.newBuilder()
		.setTransactionHandle(TransactionChecker.builder())
		.setPreparedStatementHandle(PreparedStatementChecker.builder())
		.setParameters(ParameterSetChecker.builder());
	}
	static boolean check(RequestProtos.ExecutePreparedQuery dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle())
		&& PreparedStatementChecker.check(dst.getPreparedStatementHandle())
		&& ParameterSetChecker.check(dst.getParameters());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.ExecutePreparedQuery.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class ExecuteDumpChecker {
	static final String PATH = "/This/Is/A/Path/String";
	static RequestProtos.ExecuteDump.Builder builder() {
	    return
		RequestProtos.ExecuteDump.newBuilder()
		.setTransactionHandle(TransactionChecker.builder())
		.setPreparedStatementHandle(PreparedStatementChecker.builder())
		.setParameters(ParameterSetChecker.builder())
		.setDirectory(PATH);
	}
	static boolean check(RequestProtos.ExecuteDump dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle())
		&& PreparedStatementChecker.check(dst.getPreparedStatementHandle())
		&& ParameterSetChecker.check(dst.getParameters())
		&& dst.getDirectory().equals(PATH);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.ExecuteDump.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class ExecuteLoadChecker {
	static final String PATH = "/This/Is/A/Path/String";
	static RequestProtos.ExecuteLoad.Builder builder() {
	    return
		RequestProtos.ExecuteLoad.newBuilder()
		.setTransactionHandle(TransactionChecker.builder())
		.setPreparedStatementHandle(PreparedStatementChecker.builder())
		.setParameters(ParameterSetChecker.builder())
		.addFile(PATH);
	}
	static boolean check(RequestProtos.ExecuteLoad dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle())
		&& PreparedStatementChecker.check(dst.getPreparedStatementHandle())
		&& ParameterSetChecker.check(dst.getParameters())
		&& dst.getFileList().get(0).equals(PATH);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.ExecuteLoad.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class CommitChecker {
	static RequestProtos.Commit.Builder builder() {
	    return
		RequestProtos.Commit.newBuilder()
		.setTransactionHandle(TransactionChecker.builder());
	}
	static boolean check(RequestProtos.Commit dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Commit.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class RollbackChecker {
	static RequestProtos.Rollback.Builder builder() {
	    return
		RequestProtos.Rollback.newBuilder()
		.setTransactionHandle(TransactionChecker.builder());
	}
	static boolean check(RequestProtos.Rollback dst) {
	    return
		TransactionChecker.check(dst.getTransactionHandle());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Rollback.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class DisposePreparedStatementChecker {
	static RequestProtos.DisposePreparedStatement.Builder builder() {
	    return
		RequestProtos.DisposePreparedStatement.newBuilder()
		.setPreparedStatementHandle(PreparedStatementChecker.builder());
	}
	static boolean check(RequestProtos.DisposePreparedStatement dst) {
	    return
		PreparedStatementChecker.check(dst.getPreparedStatementHandle());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.DisposePreparedStatement.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    static class DisconnectChecker {
	static RequestProtos.Disconnect.Builder builder() {
	    return
		RequestProtos.Disconnect.newBuilder();
	}
	static boolean check(RequestProtos.Disconnect dst) {
	    return
		true;
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Disconnect.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExplainChecker {
	public static RequestProtos.Explain.Builder builder() {
	    return
		RequestProtos.Explain.newBuilder()
		.setPreparedStatementHandle(PreparedStatementChecker.builder())
		.setParameters(ParameterSetChecker.builder());
	}
	static boolean check(RequestProtos.Explain dst) {
	    return
		PreparedStatementChecker.check(dst.getPreparedStatementHandle())
		&& ParameterSetChecker.check(dst.getParameters());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Explain.parseFrom(builder().build().toByteArray())));
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
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setBegin(BeginChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setBegin(BeginChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.BEGIN.equals(dst.getRequestCase())
		&& BeginChecker.check(dst.getBegin());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class PrepareRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setPrepare(PrepareChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {
	    return
		RequestProtos.Request.newBuilder()
		.setPrepare(PrepareChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.PREPARE.equals(dst.getRequestCase())
		&& PrepareChecker.check(dst.getPrepare());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExecuteStatementRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setExecuteStatement(ExecuteStatementChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setExecuteStatement(ExecuteStatementChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.EXECUTE_STATEMENT.equals(dst.getRequestCase())
		&& ExecuteStatementChecker.check(dst.getExecuteStatement());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExecuteQueryRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setExecuteQuery(ExecuteQueryChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setExecuteQuery(ExecuteQueryChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.EXECUTE_QUERY.equals(dst.getRequestCase())
		&& ExecuteQueryChecker.check(dst.getExecuteQuery());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExecutePreparedStatementRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setExecutePreparedStatement(ExecutePreparedStatementChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setExecutePreparedStatement(ExecutePreparedStatementChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.EXECUTE_PREPARED_STATEMENT.equals(dst.getRequestCase())
		&& ExecutePreparedStatementChecker.check(dst.getExecutePreparedStatement());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExecutePreparedQueryRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setExecutePreparedQuery(ExecutePreparedQueryChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setExecutePreparedQuery(ExecutePreparedQueryChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.EXECUTE_PREPARED_QUERY.equals(dst.getRequestCase())
		&& ExecutePreparedQueryChecker.check(dst.getExecutePreparedQuery());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class CommitRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setCommit(CommitChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setCommit(CommitChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.COMMIT.equals(dst.getRequestCase())
		&& CommitChecker.check(dst.getCommit());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class RollbackRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setRollback(RollbackChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setRollback(RollbackChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.ROLLBACK.equals(dst.getRequestCase())
		&& RollbackChecker.check(dst.getRollback());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class DisposePreparedStatementRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setDisposePreparedStatement(DisposePreparedStatementChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setDisposePreparedStatement(DisposePreparedStatementChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.DISPOSE_PREPARED_STATEMENT.equals(dst.getRequestCase())
		&& DisposePreparedStatementChecker.check(dst.getDisposePreparedStatement());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class DisconnectRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setDisconnect(DisconnectChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setDisconnect(DisconnectChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.DISCONNECT.equals(dst.getRequestCase())
		&& DisconnectChecker.check(dst.getDisconnect());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExplainRequestChecker {
	static RequestProtos.Request.Builder builder(long id) {
	    return
		RequestProtos.Request.newBuilder()
		.setSessionHandle(CommonProtos.Session.newBuilder().setHandle(id))
		.setExplain(ExplainChecker.builder());
	}
	public static RequestProtos.Request.Builder builder() {  // SessionHandle won't be set
	    return
		RequestProtos.Request.newBuilder()
		.setExplain(ExplainChecker.builder());
	}
	public static boolean check(RequestProtos.Request dst, long id) {
	    return
		(dst.getSessionHandle().getHandle() == id)
		&& RequestProtos.Request.RequestCase.EXPLAIN.equals(dst.getRequestCase())
		&& ExplainChecker.check(dst.getExplain());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(RequestProtos.Request.parseFrom(builder(sessionID).build().toByteArray()), sessionID));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }


    /**
     * Check of Response parts
     */
    static class SuccessChecker {
	static ResponseProtos.Success.Builder builder() {
	    return
		ResponseProtos.Success.newBuilder();
	}
	static boolean check(ResponseProtos.Success dst) {
	    return
		true;
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Success.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }
    static class ErrorChecker {
	static final String ERROR = "This is a error for test purpose";
	static ResponseProtos.Error.Builder builder() {
	    return
		ResponseProtos.Error.newBuilder()
		.setStatus(StatusProtos.Status.NOT_FOUND)
		.setDetail(ERROR);
	}
	static boolean check(ResponseProtos.Error dst) {
	    return
		dst.getStatus().equals(StatusProtos.Status.NOT_FOUND)
		&& dst.getDetail().equals(ERROR);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Error.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    /**
     * Check of each Response
     */
    public static class ResultOnlyChecker {
	static ResponseProtos.ResultOnly.Builder builder() {
	    return
		ResponseProtos.ResultOnly.newBuilder()
		.setSuccess(SuccessChecker.builder());
	}
	public static boolean check(ResponseProtos.ResultOnly dst) {
	    return
		ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(dst.getResultCase())
		&& SuccessChecker.check(dst.getSuccess());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.ResultOnly.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ResMessageBeginChecker {
	static ResponseProtos.Begin.Builder builder() {
	    return
		ResponseProtos.Begin.newBuilder()
		.setTransactionHandle(TransactionChecker.builder());
	}
	public static boolean check(ResponseProtos.Begin dst) {
	    return
		ResponseProtos.Begin.ResultCase.TRANSACTION_HANDLE.equals(dst.getResultCase())
		&& TransactionChecker.check(dst.getTransactionHandle());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Begin.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ResMessagePrepareChecker {
	static ResponseProtos.Prepare.Builder builder() {
	    return
		ResponseProtos.Prepare.newBuilder()
		.setPreparedStatementHandle(PreparedStatementChecker.builder());
	}
	public static boolean check(ResponseProtos.Prepare dst) {
	    return
		ResponseProtos.Prepare.ResultCase.PREPARED_STATEMENT_HANDLE.equals(dst.getResultCase())
		&& PreparedStatementChecker.check(dst.getPreparedStatementHandle());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Prepare.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ResMessageExecuteQueryChecker {
	static final String RESYKTSET_NAME = "ResultSetName";
	static ResponseProtos.ExecuteQuery.Builder builder() {
	    return
		ResponseProtos.ExecuteQuery.newBuilder()
		.setName(RESYKTSET_NAME)
		.setRecordMeta(SchemaProtosChecker.builder());
	}
	public static boolean check(ResponseProtos.ExecuteQuery dst) {
	    return
		dst.getName().equals(RESYKTSET_NAME)
		&& SchemaProtosChecker.check(dst.getRecordMeta());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.ExecuteQuery.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ResMessageExplainChecker {
	static final String EXPLAIN = "ThisIsAnExecutionPlanString";
	public static ResponseProtos.Explain.Builder builder() {
	    return
		ResponseProtos.Explain.newBuilder()
		.setOutput(EXPLAIN);
	}
	public static boolean check(ResponseProtos.Explain dst) {
	    return
		dst.getOutput().equals(EXPLAIN);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Explain.parseFrom(builder().build().toByteArray())));
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
	public static ResponseProtos.Response.Builder builder() {
	    return
		ResponseProtos.Response.newBuilder()
		.setResultOnly(ResultOnlyChecker.builder());
	}
	public static boolean check(ResponseProtos.Response dst) {
	    return
		ResponseProtos.Response.ResponseCase.RESULT_ONLY.equals(dst.getResponseCase())
		&& ResultOnlyChecker.check(dst.getResultOnly());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Response.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class BeginResponseChecker {
	public static ResponseProtos.Response.Builder builder() {
	    return
		ResponseProtos.Response.newBuilder()
		.setBegin(ResMessageBeginChecker.builder());
	}
	public static boolean check(ResponseProtos.Response dst) {
	    return
		ResponseProtos.Response.ResponseCase.BEGIN.equals(dst.getResponseCase())
		&& ResMessageBeginChecker.check(dst.getBegin());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Response.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class PrepareResponseChecker {
	public static ResponseProtos.Response.Builder builder() {
	    return
		ResponseProtos.Response.newBuilder()
		.setPrepare(ResMessagePrepareChecker.builder());
	}
	public static boolean check(ResponseProtos.Response dst) {
	    return
		ResponseProtos.Response.ResponseCase.PREPARE.equals(dst.getResponseCase())
		&& ResMessagePrepareChecker.check(dst.getPrepare());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Response.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExecuteQueryResponseChecker {
	public static ResponseProtos.Response.Builder builder() {
	    return
		ResponseProtos.Response.newBuilder()
		.setExecuteQuery(ResMessageExecuteQueryChecker.builder());
	}
	public static boolean check(ResponseProtos.Response dst) {
	    return
		ResponseProtos.Response.ResponseCase.EXECUTE_QUERY.equals(dst.getResponseCase())
		&& ResMessageExecuteQueryChecker.check(dst.getExecuteQuery());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Response.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }

    public static class ExplainResponseChecker {
	public static ResponseProtos.Response.Builder builder() {
	    return
		ResponseProtos.Response.newBuilder()
		.setExplain(ResMessageExplainChecker.builder());
	}
	public static boolean check(ResponseProtos.Response dst) {
	    return
		ResponseProtos.Response.ResponseCase.EXPLAIN.equals(dst.getResponseCase())
		&& ResMessageExplainChecker.check(dst.getExplain());
	}
	@Test
	void test() {
	    try {
		assertTrue(check(ResponseProtos.Response.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }


    /**
     * Check of Schema meta data
     */
    public static class SchemaProtosChecker {
	public static SchemaProtos.RecordMeta.Builder builder() {
	    return
		SchemaProtos.RecordMeta.newBuilder()
		.addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v1").setType(CommonProtos.DataType.INT8))
		.addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v2").setType(CommonProtos.DataType.FLOAT8).setNullable(false))
		.addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v3").setType(CommonProtos.DataType.CHARACTER).setNullable(true))
		.addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setType(CommonProtos.DataType.INT8))
		.addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setType(CommonProtos.DataType.FLOAT8).setNullable(false))
		.addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setType(CommonProtos.DataType.CHARACTER).setNullable(true));
	}
	public static boolean check(SchemaProtos.RecordMeta dst) {
	    SchemaProtos.RecordMeta.Column v1 = dst.getColumnsList().get(0);
	    SchemaProtos.RecordMeta.Column v2 = dst.getColumnsList().get(1);
	    SchemaProtos.RecordMeta.Column v3 = dst.getColumnsList().get(2);
	    SchemaProtos.RecordMeta.Column v4 = dst.getColumnsList().get(3);
	    SchemaProtos.RecordMeta.Column v5 = dst.getColumnsList().get(4);
	    SchemaProtos.RecordMeta.Column v6 = dst.getColumnsList().get(5);

	    return
		v1.getName().equals("v1")
		&& v1.getType().equals(CommonProtos.DataType.INT8)
		&& (v1.getNullable() ==  false)
		&& v2.getName().equals("v2")
		&& v2.getType().equals(CommonProtos.DataType.FLOAT8)
		&& (v2.getNullable() ==  false)
		&& v3.getName().equals("v3")
		&& v3.getType().equals(CommonProtos.DataType.CHARACTER)
		&& (v3.getNullable() ==  true)
		&& v4.getType().equals(CommonProtos.DataType.INT8)
		&& v4.getName().equals("")
		&& (v4.getNullable() ==  false)
		&& v5.getName().equals("")
		&& v5.getType().equals(CommonProtos.DataType.FLOAT8)
		&& (v5.getNullable() ==  false)
		&& v6.getName().equals("")
		&& v6.getType().equals(CommonProtos.DataType.CHARACTER)
		&& (v6.getNullable() ==  true)
		&& (dst.getColumnsList().size() == 6);
	}
	@Test
	void test() {
	    try {
		assertTrue(check(SchemaProtos.RecordMeta.parseFrom(builder().build().toByteArray())));
	    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
		fail("cought com.google.protobuf.InvalidProtocolBufferException");
	    }
	}
    }
}
