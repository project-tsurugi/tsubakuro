package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.value.ValueType;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;
import com.nautilus_technologies.tsubakuro.low.sql.CommonProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.junit.jupiter.api.Test;

class ResultSetImplTest {
    class ResultSetWireMock implements ResultSetWire {
	class MessagePackInputStreamMock extends MessagePackInputStream {
	    private ByteBuffer buf;
	    private int position;
	    
	    MessagePackInputStreamMock() {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		MessagePacker packer = org.msgpack.core.MessagePack.newDefaultPacker(outputStream);

		try {
		    // first column data
		    packer.packLong((long) 987654321);
		    packer.packDouble((double) 12345.6789);
		    packer.packString("This is a string for the test");
		    packer.packLong((long) 123456789);
		    packer.packDouble((double) 98765.4321);
		    packer.packNil();

		    // second column data
		    packer.packLong((long) 876543219);
		    packer.packDouble((double) 2345.67891);
		    packer.packNil();
		    packer.packLong((long) 234567891);
		    packer.packDouble((double) 8765.43219);
		    packer.packString("This is second string for the test");

		    packer.flush();
		} catch (IOException e) {
		    System.out.println("error");
		}
		byte[] ba = outputStream.toByteArray();
		var size = outputStream.size();

		buf = ByteBuffer.allocateDirect(size);
		buf.put(ba, 0, size);
		buf.rewind();
		position = 0;
	    }

	    public synchronized int read() throws IOException {
		if (!buf.hasRemaining()) {
		    return -1;
		}
		return buf.get();
	    }
	    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
		if (!buf.hasRemaining()) {
		    return -1;
		}
		len = Math.min(len, buf.remaining());
		buf.get(bytes, off, len);
		return len;
	    }
	    public void dispose(long length) {
		position += length;
		buf.position(position);
	    }
	}

	MessagePackInputStreamMock msgPackInputStreamMock;

	ResultSetWireMock() {
	    msgPackInputStreamMock = new MessagePackInputStreamMock();
	}

	public SchemaProtos.RecordMeta receiveSchemaMetaData() throws IOException {
	    return ProtosForTest.SchemaProtosChecker.builder().build();
	}

	public MessagePackInputStream getMessagePackInputStream() {
	    return msgPackInputStreamMock;
	}

	public void close() throws IOException {
	}
    }

    private ResultSetImpl resultSetImpl;

    @Test
    void receiveSchemaMetaAndRecord() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock());

	    recordMeta = resultSetImpl.getRecordMeta();
	    assertEquals(recordMeta.fieldCount(), 6);
	    assertEquals(recordMeta.at(0), CommonProtos.DataType.INT8);
	    assertFalse(recordMeta.nullable(0));
	    assertEquals(recordMeta.at(1), CommonProtos.DataType.FLOAT8);
	    assertFalse(recordMeta.nullable(1));
	    assertEquals(recordMeta.at(2), CommonProtos.DataType.STRING);
	    assertTrue(recordMeta.nullable(2));
	    assertEquals(recordMeta.at(3), CommonProtos.DataType.INT8);
	    assertFalse(recordMeta.nullable(3));
	    assertEquals(recordMeta.at(4), CommonProtos.DataType.FLOAT8);
	    assertFalse(recordMeta.nullable(4));
	    assertEquals(recordMeta.at(5), CommonProtos.DataType.STRING);
	    assertTrue(recordMeta.nullable(5));

	    // first column data
	    assertTrue(resultSetImpl.nextRecord());
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getInt8(), 987654321L);
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getFloat8(), (double) 12345.6789);
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getCharacter(), "This is a string for the test");
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getInt8(), (long) 123456789L);
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getFloat8(), (double) 98765.4321);
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.isNull());
	    assertFalse(resultSetImpl.nextColumn());

	    // second column data
	    assertTrue(resultSetImpl.nextRecord());
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getInt8(), 876543219L);
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getFloat8(), (double) 2345.67891);
	    assertTrue(resultSetImpl.nextColumn());
	    assertTrue(resultSetImpl.isNull());
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getInt8(), (long) 234567891L);
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getFloat8(), (double) 8765.43219);
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getCharacter(), "This is second string for the test");
	    assertFalse(resultSetImpl.nextColumn());

	    // end of records
	    assertFalse(resultSetImpl.nextRecord());
	    
	} catch (IOException e) {
            fail("cought IOException");
        }
    }
}
