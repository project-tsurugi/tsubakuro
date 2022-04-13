package com.nautilus_technologies.tsubakuro.impl.low.sql;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Future;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.ByteBufferInput;

import com.nautilus_technologies.tsubakuro.low.sql.ResultSet;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.protos.ResponseProtos;
import com.nautilus_technologies.tsubakuro.protos.CommonProtos;
import com.nautilus_technologies.tsubakuro.protos.ProtosForTest;

import org.junit.jupiter.api.Test;

class ResultSetImplTest {
    class ResultSetWireMock implements ResultSetWire {
	private ByteBufferBackedInputMock byteBufferInput;
	private ByteBuffer buf;

	class ByteBufferBackedInputMock extends ByteBufferInput {
	    ByteBufferBackedInputMock(ByteBuffer byteBuffer) {
		super(byteBuffer);
	    }

	    boolean disposeUsedData(long length) {
		var size = buf.capacity() - (int) length;
		if (size == 0) {
		    return false;
		}

		var newBuf = ByteBuffer.allocate(size);  // buffer created by allocateDirect() does not support .array(), so it is not appropriate here
		newBuf.put(buf.array(), (int) length, size);
		newBuf.rewind();
		buf = newBuf;
		reset(newBuf);
		return true;
	    }
	}

	ResultSetWireMock() {
	    byteBufferInput = null;

	    byte[] ba = createRecordsForTest();
	    var length = ba.length;

	    buf = ByteBuffer.allocate(length);  // buffer created by allocateDirect() does not support .array(), so it is not appropriate here
	    buf.put(ba, 0, length);
	    buf.rewind();
	}

	public ByteBufferInput getByteBufferBackedInput() {
	    if (Objects.isNull(byteBufferInput)) {
		byteBufferInput = new ByteBufferBackedInputMock(buf);
	    }
	    return byteBufferInput;
	}

	public boolean disposeUsedData(long length) {
	    return byteBufferInput.disposeUsedData(length);
	}

	public void connect(String name) throws IOException {
	}

	public void close() throws IOException {
	}
    }

    private ResultSetImpl resultSetImpl;

    byte[] createRecordsForTest() {
	try {
	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	    MessagePacker packer = org.msgpack.core.MessagePack.newDefaultPacker(outputStream);

	    // first record data
	    packer.packLong((long) 987654321);
	    packer.packDouble((double) 12345.6789);
	    packer.packString("This is a string for the test");
	    packer.packLong((long) 123456789);
	    packer.packDouble((double) 98765.4321);
	    packer.packNil();

	    // second record data
	    packer.packLong((long) 876543219);
	    packer.packDouble((double) 2345.67891);
	    packer.packNil();
	    packer.packLong((long) 234567891);
	    packer.packDouble((double) 8765.43219);
	    packer.packString("This is second string for the test");

	    packer.flush();
	    return outputStream.toByteArray();
	} catch (IOException e) {
	    System.out.println(e);
	}
	return null;
    }

    @Test
    void receiveRecord() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

	    // first column data
	    assertTrue(resultSetImpl.nextRecord());
	    {
		Throwable exception = assertThrows(IOException.class, () -> {
			var n = resultSetImpl.name();
		    });
		assertEquals("the column is not ready to be read", exception.getMessage());
	    }
	    {
		Throwable exception = assertThrows(IOException.class, () -> {
			var n = resultSetImpl.type();
		    });
		assertEquals("the column is not ready to be read", exception.getMessage());
	    }
	    {
		Throwable exception = assertThrows(IOException.class, () -> {
			var n = resultSetImpl.nullable();
		    });
		assertEquals("the column is not ready to be read", exception.getMessage());
	    }

	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.name(), "v1");
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.INT8);
	    assertEquals(resultSetImpl.nullable(), false);
	    assertEquals(resultSetImpl.getInt8(), 987654321L);
	    assertEquals(resultSetImpl.name(), "v1");
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.INT8);
	    assertEquals(resultSetImpl.nullable(), false);

	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.name(), "v2");
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.FLOAT8);
	    assertEquals(resultSetImpl.nullable(), false);
	    assertEquals(resultSetImpl.getFloat8(), (double) 12345.6789);
	    assertEquals(resultSetImpl.name(), "v2");
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.FLOAT8);
	    assertEquals(resultSetImpl.nullable(), false);

	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.name(), "v3");
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.CHARACTER);
	    assertEquals(resultSetImpl.nullable(), true);
	    assertEquals(resultSetImpl.getCharacter(), "This is a string for the test");
	    assertEquals(resultSetImpl.name(), "v3");
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.CHARACTER);
	    assertEquals(resultSetImpl.nullable(), true);

	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.name(), "");  // no name
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.INT8);
	    assertEquals(resultSetImpl.nullable(), false);
	    assertEquals(resultSetImpl.getInt8(), (long) 123456789L);
	    assertEquals(resultSetImpl.name(), "");  // no name
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.INT8);
	    assertEquals(resultSetImpl.nullable(), false);

	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.name(), "");  // no name
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.FLOAT8);
	    assertEquals(resultSetImpl.nullable(), false);
	    assertEquals(resultSetImpl.getFloat8(), (double) 98765.4321);
	    assertEquals(resultSetImpl.name(), "");  // no name
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.FLOAT8);
	    assertEquals(resultSetImpl.nullable(), false);
	    
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.name(), "");  // no name
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.CHARACTER);
	    assertEquals(resultSetImpl.nullable(), true);
	    assertTrue(resultSetImpl.isNull());
	    assertEquals(resultSetImpl.name(), "");  // no name
	    assertEquals(resultSetImpl.type(), CommonProtos.DataType.CHARACTER);
	    assertEquals(resultSetImpl.nullable(), true);

	    assertFalse(resultSetImpl.nextColumn());
	    {
		Throwable exception = assertThrows(IOException.class, () -> {
			var n = resultSetImpl.name();
		    });
		assertEquals("the column is not ready to be read", exception.getMessage());
	    }
	    {
		Throwable exception = assertThrows(IOException.class, () -> {
			var n = resultSetImpl.type();
		    });
		assertEquals("the column is not ready to be read", exception.getMessage());
	    }
	    {
		Throwable exception = assertThrows(IOException.class, () -> {
			var n = resultSetImpl.nullable();
		    });
		assertEquals("the column is not ready to be read", exception.getMessage());
	    }

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

    @Test
    void receiveAndRecordWithSkip() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

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
	    // skip the rest of columns (pattern 1)
	    
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
	    // skip the rest of columns (pattern 2)

	    // end of records
	    assertFalse(resultSetImpl.nextRecord());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void receiveAndRecordWithSkip2() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

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
	    // skip the rest of columns (pattern 1)
	    
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
	    // skip the rest of columns (pattern 2)

	    // end of records
	    assertFalse(resultSetImpl.nextRecord());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void getColumnWithoutNextColumn() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

	    // first column data
	    assertTrue(resultSetImpl.nextRecord());
	    assertTrue(resultSetImpl.nextColumn());
	    assertEquals(resultSetImpl.getInt8(), 987654321L);

	    Throwable exception = assertThrows(IOException.class, () -> {
		    var f = resultSetImpl.getFloat8();
		});
	    assertEquals("the column is not ready to be read", exception.getMessage());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void getColumnInDifferntType() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

	    // first column data
	    assertTrue(resultSetImpl.nextRecord());
	    assertTrue(resultSetImpl.nextColumn());

	    Throwable exception = assertThrows(IOException.class, () -> {
		    var f = resultSetImpl.getFloat8();
		});
	    assertEquals("the column type is not what is expected", exception.getMessage());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void getColumnThatIsNull() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

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

	    Throwable exception = assertThrows(IOException.class, () -> {
		    var s = resultSetImpl.getCharacter();
		});
	    assertEquals("the column is Null", exception.getMessage());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void getColumnThatIsNullPattern2() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

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

	    Throwable exception = assertThrows(IOException.class, () -> {
		    var s = resultSetImpl.getCharacter();
		});
	    assertEquals("the column is Null", exception.getMessage());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void nextRecordAfterIsNull() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

	    // first record
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

	    // second record
	    assertTrue(resultSetImpl.nextRecord());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void getColumnWithoutNextRecord() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

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
	    
	    Throwable exception = assertThrows(IOException.class, () -> {
		    var i = resultSetImpl.getInt8();
		});
	    assertEquals("the column is not ready to be read", exception.getMessage());

	} catch (IOException e) {
            fail("cought IOException");
        }
    }

    @Test
    void useAfterClose() {
	ResultSet.RecordMeta recordMeta;
        try {
	    resultSetImpl = new ResultSetImpl(new ResultSetWireMock(), (Future<ResponseProtos.ResultOnly>) null);
	    resultSetImpl.connect("dummy", ProtosForTest.SchemaProtosChecker.builder().build());

	    // first column data
	    resultSetImpl.nextRecord();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getInt8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getFloat8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getCharacter();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getInt8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getFloat8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.isNull();
	    resultSetImpl.nextColumn();

	    // second column data
	    resultSetImpl.nextRecord();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getInt8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getFloat8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.isNull();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getInt8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getFloat8();
	    resultSetImpl.nextColumn();
	    resultSetImpl.getCharacter();
	    resultSetImpl.nextColumn();

	    // end of records
	    resultSetImpl.nextRecord();
	    resultSetImpl.close();

	} catch (IOException e) {
	    System.out.println(e);
            fail("cought IOException");
        }

	Throwable exception = assertThrows(IOException.class, () -> {
		resultSetImpl.nextRecord();
	    });
	assertEquals("already closed", exception.getMessage());
   }
}
