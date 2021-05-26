package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import com.nautilus_technologies.tsubakuro.low.sql.RequestProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;

import org.junit.jupiter.api.Test;

class RequestProtosTest {

    @Test
    void placeHolder() {
	RequestProtos.PlaceHolder src = RequestProtos.PlaceHolder.newBuilder()
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("v1").setType(CommonProtos.DataType.INT8).build())
	    .addVariables(RequestProtos.PlaceHolder.Variable.newBuilder().setName("v2").setType(CommonProtos.DataType.FLOAT8).build())
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
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("v1").setIValue(11).build())
	    .addParameters(RequestProtos.ParameterSet.Parameter.newBuilder().setName("v2").setDValue(123.45).build())
	    .build();

	byte[] data = src.toByteArray();
	
	try {
	    RequestProtos.ParameterSet dst = RequestProtos.ParameterSet.parseFrom(data);

	    RequestProtos.ParameterSet.Parameter v1 = dst.getParametersList().get(0);	
	    RequestProtos.ParameterSet.Parameter v2 = dst.getParametersList().get(1);
	
	    assertAll(
		      () -> assertEquals(v1.getName(), "v1"),
		      () -> assertTrue(RequestProtos.ParameterSet.Parameter.ValueCase.I_VALUE.equals(v1.getValueCase())),
		      () -> assertEquals(v2.getName(), "v2"),
		      () -> assertTrue(RequestProtos.ParameterSet.Parameter.ValueCase.D_VALUE.equals(v2.getValueCase())),
		      () -> assertEquals(dst.getParametersList().size(), 2));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }
}
