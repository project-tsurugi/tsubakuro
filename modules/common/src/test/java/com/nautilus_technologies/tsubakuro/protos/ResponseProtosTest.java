package com.nautilus_technologies.tsubakuro.protos;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ResponseProtosTest {

    @Test
    void resultOnlySuccess() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setResultOnly(ResponseProtos.ResultOnly.newBuilder()
			   .setSuccess(ResponseProtos.Success.newBuilder()))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.RESULT_ONLY.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.ResultOnly.ResultCase.SUCCESS.equals(dst.getResultOnly().getResultCase())));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }

    @Test
    void resultOnlyError() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setResultOnly(ResponseProtos.ResultOnly.newBuilder()
			   .setError(ResponseProtos.Error.newBuilder().setDetail("This is a error for test")))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.RESULT_ONLY.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.ResultOnly.ResultCase.ERROR.equals(dst.getResultOnly().getResultCase())),
		      () -> assertEquals(dst.getResultOnly().getError().getDetail(), "This is a error for test"));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }


    @Test
    void beginSuccess() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setBegin(ResponseProtos.Begin.newBuilder()
		      .setTransactionHandle(CommonProtos.Transaction.newBuilder().setHandle(123)))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.BEGIN.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.Begin.ResultCase.TRANSACTION_HANDLE.equals(dst.getBegin().getResultCase())),
		      () -> assertEquals(dst.getBegin().getTransactionHandle().getHandle(), 123));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }

    @Test
    void beginError() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setBegin(ResponseProtos.Begin.newBuilder()
			   .setError(ResponseProtos.Error.newBuilder().setDetail("This is a error for test")))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.BEGIN.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.Begin.ResultCase.ERROR.equals(dst.getBegin().getResultCase())),
		      () -> assertEquals(dst.getBegin().getError().getDetail(), "This is a error for test"));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }


    @Test
    void prepareSuccess() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setPrepare(ResponseProtos.Prepare.newBuilder()
			.setPreparedStatementHandle(CommonProtos.PreparedStatement.newBuilder().setHandle(456)))
	    .build();
	
	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.PREPARE.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.Prepare.ResultCase.PREPARED_STATEMENT_HANDLE.equals(dst.getPrepare().getResultCase())),
		      () -> assertEquals(dst.getPrepare().getPreparedStatementHandle().getHandle(), 456));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }

    @Test
    void prepareError() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setPrepare(ResponseProtos.Prepare.newBuilder()
			   .setError(ResponseProtos.Error.newBuilder().setDetail("This is a error for test")))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.PREPARE.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.Prepare.ResultCase.ERROR.equals(dst.getPrepare().getResultCase())),
		      () -> assertEquals(dst.getPrepare().getError().getDetail(), "This is a error for test"));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }


    @Test
    void executeQuerySuccess() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setExecuteQuery(ResponseProtos.ExecuteQuery.newBuilder()
			     .setResultSetInfo(ResponseProtos.ResultSetInfo.newBuilder()
					       .setName("nameOfTheResult")
					       .setRecordMeta(SchemaProtos.RecordMeta.newBuilder()
							      .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v1").setType(CommonProtos.DataType.INT8))
							      .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v2").setType(CommonProtos.DataType.FLOAT8))
							      .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v3").setType(CommonProtos.DataType.CHARACTER).setNullable(true))
							      .build())
					       )
			     ).build();

	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);
	    var recordMeta = dst.getExecuteQuery().getResultSetInfo().getRecordMeta();
            SchemaProtos.RecordMeta.Column v1 = recordMeta.getColumnsList().get(0);
            SchemaProtos.RecordMeta.Column v2 = recordMeta.getColumnsList().get(1);
            SchemaProtos.RecordMeta.Column v3 = recordMeta.getColumnsList().get(2);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.EXECUTE_QUERY.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.ExecuteQuery.ResultCase.RESULT_SET_INFO.equals(dst.getExecuteQuery().getResultCase())),
		      () -> assertEquals(dst.getExecuteQuery().getResultSetInfo().getName(), "nameOfTheResult"),
                      () -> assertEquals(v1.getName(), "v1"),
                      () -> assertEquals(v1.getType(), CommonProtos.DataType.INT8),
                      () -> assertEquals(v1.getNullable(), false),
                      () -> assertEquals(v2.getName(), "v2"),
                      () -> assertEquals(v2.getType(), CommonProtos.DataType.FLOAT8),
                      () -> assertEquals(v2.getNullable(), false),
                      () -> assertEquals(v3.getName(), "v3"),
                      () -> assertEquals(v3.getType(), CommonProtos.DataType.CHARACTER),
                      () -> assertEquals(v3.getNullable(), true),
                      () -> assertEquals(recordMeta.getColumnsList().size(), 3));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }

    @Test
    void executeQueryError() {
	ResponseProtos.Response src = ResponseProtos.Response.newBuilder()
	    .setExecuteQuery(ResponseProtos.ExecuteQuery.newBuilder()
			   .setError(ResponseProtos.Error.newBuilder().setDetail("This is a error for test")))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    ResponseProtos.Response dst = ResponseProtos.Response.parseFrom(data);

	    assertAll(
		      () -> assertTrue(ResponseProtos.Response.ResponseCase.EXECUTE_QUERY.equals(dst.getResponseCase())),
		      () -> assertTrue(ResponseProtos.ExecuteQuery.ResultCase.ERROR.equals(dst.getExecuteQuery().getResultCase())),
		      () -> assertEquals(dst.getExecuteQuery().getError().getDetail(), "This is a error for test"));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }
}
