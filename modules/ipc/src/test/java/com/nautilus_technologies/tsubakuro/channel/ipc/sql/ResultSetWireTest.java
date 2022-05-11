package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ValueType;

import com.nautilus_technologies.tsubakuro.channel.ipc.SessionWireImpl;

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
	    var input = resultSetWire.getByteBufferBackedInput();
	    var unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(input);

	    assertTrue(unpacker.hasNext());

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

	    // next record exists
	    assertTrue(resultSetWire.disposeUsedData(unpacker.getTotalReadBytes()));

	    // second record data
	    assertTrue(unpacker.hasNext());

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

	    // end of record
	    assertFalse(resultSetWire.disposeUsedData(unpacker.getTotalReadBytes()));

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
	    var input = resultSetWire.getByteBufferBackedInput();

	    assertTrue(Objects.isNull(input));
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
