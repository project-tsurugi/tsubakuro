package com.nautilus_technologies.tsubakuro.protos;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RequestProtosTest {

    @Test
    void placeHolder() {
	RequestProtos.PlaceHolder src = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("v1").setType(CommonProtos.DataType.INT8))
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("v2").setType(CommonProtos.DataType.FLOAT8))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    RequestProtos.PlaceHolder dst = RequestProtos.PlaceHolder.parseFrom(data);

	    RequestProtos.PlaceHolder.Variable v1 = dst.getVariablesList().get(0);	
	    RequestProtos.PlaceHolder.Variable v2 = dst.getVariablesList().get(1);
	
	    assertAll(
		      () -> assertEquals(v1.getName(), "v1"),
		      () -> assertEquals(v1.getType(), CommonProtos.DataType.INT8),
		      () -> assertEquals(v2.getName(), "v2"),
		      () -> assertEquals(v2.getType(), CommonProtos.DataType.FLOAT8),
		      () -> assertEquals(dst.getVariablesList().size(), 2));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }

    @Test
    void parameterSet() {
	RequestProtos.ParameterSet src = RequestProtos.ParameterSet.newBuilder()
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("v1").setInt4Value(11))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("v2").setFloat8Value(123.45))
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("v3"))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    RequestProtos.ParameterSet dst = RequestProtos.ParameterSet.parseFrom(data);

	    RequestProtos.ParameterSet.Parameter v1 = dst.getParametersList().get(0);	
	    RequestProtos.ParameterSet.Parameter v2 = dst.getParametersList().get(1);
	    RequestProtos.ParameterSet.Parameter v3 = dst.getParametersList().get(2);
	
	    assertAll(
		      () -> assertEquals(v1.getName(), "v1"),
		      () -> assertTrue(RequestProtos.ParameterSet.Parameter.ValueCase.INT4_VALUE.equals(v1.getValueCase())),
		      () -> assertEquals(v2.getName(), "v2"),
		      () -> assertTrue(RequestProtos.ParameterSet.Parameter.ValueCase.FLOAT8_VALUE.equals(v2.getValueCase())),
		      () -> assertEquals(v3.getName(), "v3"),
		      () -> assertTrue(RequestProtos.ParameterSet.Parameter.ValueCase.VALUE_NOT_SET.equals(v3.getValueCase())),
		      () -> assertEquals(dst.getParametersList().size(), 3));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }

    @Test
    void transactionOption() {
	RequestProtos.TransactionOption src = RequestProtos.TransactionOption.newBuilder()
	    .setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_SHORT)
	    .addWritePreserves(RequestProtos.TransactionOption.WritePreserve.newBuilder().setName("table_for_preserve"))
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    RequestProtos.TransactionOption dst = RequestProtos.TransactionOption.parseFrom(data);

	    RequestProtos.TransactionOption.WritePreserve r1 = dst.getWritePreservesList().get(0);
	
	    assertAll(
		      () -> assertEquals(dst.getType(), RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_SHORT),
		      () -> assertEquals(r1.getName(), "table_for_preserve"),
		      () -> assertEquals(dst.getWritePreservesList().size(), 1));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }


    @Test
    void begin() {
	RequestProtos.Request src = RequestProtos.Request.newBuilder()
	    .setSessionHandle(CommonProtos.Session.newBuilder().setHandle(123))
	    .setBegin(RequestProtos.Begin.newBuilder()
		      .setOption(RequestProtos.TransactionOption.newBuilder().setType(RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_READ_ONLY))
		      )
	    .build();

	byte[] data = src.toByteArray();

	try {
	    RequestProtos.Request dst = RequestProtos.Request.parseFrom(data);

	    assertAll(
		      () -> assertEquals(dst.getSessionHandle().getHandle(), 123),
		      () -> assertEquals(dst.getBegin().getOption().getType(), RequestProtos.TransactionOption.TransactionType.TRANSACTION_TYPE_READ_ONLY),
		      () -> assertTrue(RequestProtos.Request.RequestCase.BEGIN.equals(dst.getRequestCase())));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }
}
