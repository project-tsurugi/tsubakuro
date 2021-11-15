package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.ExecutionException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.value.ValueType;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

import org.junit.jupiter.api.Test;

class ResultSetWireTest {
    private SessionWireImpl client;
    private ServerWireImpl server;
    private String dbName = "tsubakuro";
    private long sessionID = 1;

    byte[] createRecordsForTest(int index) throws IOException {
	ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	MessagePacker packer = org.msgpack.core.MessagePack.newDefaultPacker(outputStream);

	if (index == 1) {
	    // first record data
	    packer.packLong((long) 987654321);
	    packer.packDouble((double) 12345.6789);
	    packer.packString("This is a string for the test");
	    packer.packLong((long) 123456789);
	    packer.packDouble((double) 98765.4321);
	    packer.packNil();
	} else if (index == 2) {
	    // second record data
	    packer.packLong((long) 876543219);
	    packer.packDouble((double) 2345.67891);
	    packer.packNil();
	    packer.packLong((long) 234567891);
	    packer.packDouble((double) 8765.43219);
	    packer.packString("This is second string for the test");
	}

	packer.flush();
	return outputStream.toByteArray();
    }

    @Test
    void records() {
	try {
	    server = new ServerWireImpl(dbName, sessionID);
	    client = new SessionWireImpl(dbName, sessionID);

	    // server side create RSL
	    long rsHandle = server.createRSL("resultset-1");

	    // server side send Records
	    server.putRecordsRSL(rsHandle, createRecordsForTest(1));
	    server.putRecordsRSL(rsHandle, createRecordsForTest(2));
	    server.eorRSL(rsHandle);

	    // client create RSL
	    var resultSetWire = client.createResultSetWire();
	    resultSetWire.connect("resultset-1");

	    // client side receive Records
	    // first record data
	    var inputStream = resultSetWire.getMessagePackInputStream();
	    var unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(inputStream);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
	    assertEquals(unpacker.unpackLong(), 987654321L);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
	    assertEquals(unpacker.unpackDouble(), (double) 12345.6789);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.STRING);
	    assertEquals(unpacker.unpackString(), "This is a string for the test");

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
	    assertEquals(unpacker.unpackLong(), 123456789L);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
	    assertEquals(unpacker.unpackDouble(), (double) 98765.4321);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.NIL);
	    unpacker.unpackNil();

	    inputStream.disposeUsedData(unpacker.getTotalReadBytes());

	    // second record data
	    unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(inputStream);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
	    assertEquals(unpacker.unpackLong(), 876543219L);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
	    assertEquals(unpacker.unpackDouble(), (double) 2345.67891);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.NIL);
	    unpacker.unpackNil();

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
	    assertEquals(unpacker.unpackLong(), 234567891L);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
	    assertEquals(unpacker.unpackDouble(), (double) 8765.43219);

	    assertEquals(unpacker.getNextFormat().getValueType(), ValueType.STRING);
	    assertEquals(unpacker.unpackString(), "This is second string for the test");

	    inputStream.disposeUsedData(unpacker.getTotalReadBytes());

	    // end of record
	    unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(inputStream);

	    assertFalse(unpacker.hasNext());
	    // RESPONSE test end

	    client.close();
	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	}
    }

    @Test
    void noRecord() {
	try {
	    server = new ServerWireImpl(dbName, sessionID);
	    client = new SessionWireImpl(dbName, sessionID);

	    // server side create RSL
	    long rsHandle = server.createRSL("resultset-1");

	    // server side send Records
	    server.eorRSL(rsHandle);

	    // client create RSL
	    var resultSetWire = client.createResultSetWire();
	    resultSetWire.connect("resultset-1");

	    // client side receive Records
	    // first record data
	    var inputStream = resultSetWire.getMessagePackInputStream();
	    var unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(inputStream);

	    assertFalse(unpacker.hasNext());
	    // RESPONSE test end

	    client.close();
	    server.close();
	} catch (IOException e) {
	    fail("cought IOException");
	}
    }

    @Test
    void notExist() {
	try {
	    server = new ServerWireImpl(dbName, sessionID);
	    client = new SessionWireImpl(dbName, sessionID);

	    // server side create RSL
	    long rsHandle = server.createRSL("resultset-1");

	    // server side send Records
	    server.putRecordsRSL(rsHandle, createRecordsForTest(1));
	    server.putRecordsRSL(rsHandle, createRecordsForTest(2));
	    server.eorRSL(rsHandle);
	} catch (IOException e) {
	    fail("cought IOException");
	}

	Throwable exception = assertThrows(IOException.class, () -> {
		var resultSetWire = client.createResultSetWire();
		resultSetWire.connect("resultset-2");  // not exist
	    });
	assertEquals("cannot find a result_set wire with the specified name: resultset-2", exception.getMessage());
    }
}
