package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ValueType;

import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

class ResultSetTotalTest {
    static final long SERVICE_ID_SQL = 3;
    private SessionWireImpl client;
    private ServerWireImpl server;
    private final String dbName = "tsubakuro";
    private final long sessionID = 1;

    byte[] createRecordsForTest(int index) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MessagePacker packer = org.msgpack.core.MessagePack.newDefaultPacker(outputStream);

        if (index == 1) {
            // first record data
            packer.packLong(987654321);
            packer.packDouble(12345.6789);
            packer.packString("This is a string for the test");
            packer.packLong(123456789);
            packer.packDouble(98765.4321);
            packer.packNil();
        } else if (index == 2) {
            // second record data
            packer.packLong(876543219);
            packer.packDouble(2345.67891);
            packer.packNil();
            packer.packLong(234567891);
            packer.packDouble(8765.43219);
            packer.packString("This is second string for the test");
        }

        packer.flush();
        return outputStream.toByteArray();
    }

    @Test
    void requestAndResponseLevel() throws Exception {
        server = new ServerWireImpl(dbName, sessionID);
        client = new SessionWireImpl(dbName, sessionID);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.sendQuery(SERVICE_ID_SQL, ProtosForTest.ExecuteQueryRequestChecker.builder());
        // server side receive Request
        assertTrue(ProtosForTest.ExecuteQueryRequestChecker.check(server.get(), sessionID));
        // REQUEST test end

        // RESPONSE test begin
        // server side send Response
        var responseToBeSent = ProtosForTest.ExecuteQueryResponseChecker.builder().build();
        server.put(responseToBeSent);

        // server side send SchemaMeta
        long rsHandle = server.createRSL(responseToBeSent.getExecuteQuery().getName());

        // server side send Records
        server.putRecordsRSL(rsHandle, createRecordsForTest(1));
        server.putRecordsRSL(rsHandle, createRecordsForTest(2));
        server.eorRSL(rsHandle);

        // server side send query result on ResponseProtos ResultOnly
        server.put(ProtosForTest.ResultOnlyResponseChecker.builder().build());


        // client side receive Response
        var responseReceived = futureResponse.getLeft().get();
        assertTrue(ProtosForTest.ResMessageExecuteQueryChecker.check(responseReceived));

        // client side receive SchemaMeta
        var resultSetWire = client.createResultSetWire();
        resultSetWire.connect(responseReceived.getName());
        var schemaMeta = responseReceived.getRecordMeta();
        assertTrue(ProtosForTest.SchemaProtosChecker.check(schemaMeta));

        // client side receive Records
        // first record data
        var input = resultSetWire.getByteBufferBackedInput();
        var unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(input);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
        assertEquals(unpacker.unpackLong(), 987654321L);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
        assertEquals(unpacker.unpackDouble(), 12345.6789);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.STRING);
        assertEquals(unpacker.unpackString(), "This is a string for the test");

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
        assertEquals(unpacker.unpackLong(), 123456789L);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
        assertEquals(unpacker.unpackDouble(), 98765.4321);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.NIL);
        unpacker.unpackNil();

        resultSetWire.disposeUsedData(unpacker.getTotalReadBytes());

        // second record data
        unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(input);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
        assertEquals(unpacker.unpackLong(), 876543219L);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
        assertEquals(unpacker.unpackDouble(), 2345.67891);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.NIL);
        unpacker.unpackNil();

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.INTEGER);
        assertEquals(unpacker.unpackLong(), 234567891L);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.FLOAT);
        assertEquals(unpacker.unpackDouble(), 8765.43219);

        assertEquals(unpacker.getNextFormat().getValueType(), ValueType.STRING);
        assertEquals(unpacker.unpackString(), "This is second string for the test");

        resultSetWire.disposeUsedData(unpacker.getTotalReadBytes());

        // end of record
        unpacker = org.msgpack.core.MessagePack.newDefaultUnpacker(input);

        assertFalse(unpacker.hasNext());

        var responseResultOnly = futureResponse.getRight().get();
        assertTrue(ProtosForTest.ResultOnlyChecker.check(responseResultOnly));
        // RESPONSE test end

        client.close();
        server.close();
    }

    @Test
    void requestAndNoResponseLevel() throws Exception {
        server = new ServerWireImpl(dbName, sessionID);
        client = new SessionWireImpl(dbName, sessionID);

        // REQUEST test begin
        // client side send Request
        var futureResponse = client.sendQuery(SERVICE_ID_SQL, ProtosForTest.ExecuteQueryRequestChecker.builder());
        // server side receive Request
        assertTrue(ProtosForTest.ExecuteQueryRequestChecker.check(server.get(), sessionID));
        // REQUEST test end


        // RESPONSE test begin
        // server side send ResultOnly Response without ExecuteQueryResponse
        server.put(ProtosForTest.ResultOnlyResponseChecker.builder().build());

        // client side receive Response
        assertTrue(Objects.isNull(futureResponse.getLeft().get()));

        var responseResultOnly = futureResponse.getRight().get();
        assertTrue(ProtosForTest.ResultOnlyChecker.check(responseResultOnly));
        // RESPONSE test end

        client.close();
        server.close();
    }
}
