package com.nautilus_technologies.tsubakuro.protos;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

//FIXME: delete tests for generated sources
class SchemaProtosTest {

    @Test
    void recordMetaWithName() {
	SchemaProtos.RecordMeta src = SchemaProtos.RecordMeta.newBuilder()
	    .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v1").setType(CommonProtos.DataType.INT8))
	    .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v2").setType(CommonProtos.DataType.FLOAT8))
	    .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setName("v3").setType(CommonProtos.DataType.CHARACTER).setNullable(true))
	    .build();

	byte[] data = src.toByteArray();

	try {
	    SchemaProtos.RecordMeta dst = SchemaProtos.RecordMeta.parseFrom(data);

	    SchemaProtos.RecordMeta.Column v1 = dst.getColumnsList().get(0);
	    SchemaProtos.RecordMeta.Column v2 = dst.getColumnsList().get(1);
	    SchemaProtos.RecordMeta.Column v3 = dst.getColumnsList().get(2);

	    assertAll(
		      () -> assertEquals(v1.getName(), "v1"),
		      () -> assertEquals(v1.getType(), CommonProtos.DataType.INT8),
		      () -> assertEquals(v1.getNullable(), false),
		      () -> assertEquals(v2.getName(), "v2"),
		      () -> assertEquals(v2.getType(), CommonProtos.DataType.FLOAT8),
		      () -> assertEquals(v2.getNullable(), false),
		      () -> assertEquals(v3.getName(), "v3"),
		      () -> assertEquals(v3.getType(), CommonProtos.DataType.CHARACTER),
		      () -> assertEquals(v3.getNullable(), true),
		      () -> assertEquals(dst.getColumnsList().size(), 3));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }

    @Test
    void recordMetaWithoutName() {
	SchemaProtos.RecordMeta src = SchemaProtos.RecordMeta.newBuilder()
	    .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setType(CommonProtos.DataType.INT8))
	    .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setType(CommonProtos.DataType.FLOAT8))
	    .addColumns(SchemaProtos.RecordMeta.Column.newBuilder().setType(CommonProtos.DataType.CHARACTER).setNullable(true))
	    .build();

	byte[] data = src.toByteArray();

	try {
	    SchemaProtos.RecordMeta dst = SchemaProtos.RecordMeta.parseFrom(data);

	    SchemaProtos.RecordMeta.Column v1 = dst.getColumnsList().get(0);
	    SchemaProtos.RecordMeta.Column v2 = dst.getColumnsList().get(1);
	    SchemaProtos.RecordMeta.Column v3 = dst.getColumnsList().get(2);

	    assertAll(
		      () -> assertEquals(v1.getName(), ""),
		      () -> assertEquals(v1.getType(), CommonProtos.DataType.INT8),
		      () -> assertEquals(v1.getNullable(), false),
		      () -> assertEquals(v2.getName(), ""),
		      () -> assertEquals(v2.getType(), CommonProtos.DataType.FLOAT8),
		      () -> assertEquals(v2.getNullable(), false),
		      () -> assertEquals(v3.getName(), ""),
		      () -> assertEquals(v3.getType(), CommonProtos.DataType.CHARACTER),
		      () -> assertEquals(v3.getNullable(), true),
		      () -> assertEquals(dst.getColumnsList().size(), 3));
	} catch (com.google.protobuf.InvalidProtocolBufferException e) {
	    fail("cought com.google.protobuf.InvalidProtocolBufferException");
	}
    }
}
