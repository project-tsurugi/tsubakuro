package com.nautilus_technologies.tsubakuro.impl.low.sql;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import com.nautilus_technologies.tsubakuro.low.sql.SchemaProtos;
import com.nautilus_technologies.tsubakuro.low.sql.ProtosForTest;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.value.ValueType;

public class ResultSetWireMock implements ResultSetWire {
    class MsgPackInputStreamMock extends MsgPackInputStream {
	private ByteBuffer buf;

	MsgPackInputStreamMock() {
	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	    MessagePacker packer = org.msgpack.core.MessagePack.newDefaultPacker(outputStream);

	    try {
		// column data here
		packer.packLong((long) 987654321);
		packer.packDouble((double) 12345.6789);
		packer.packString("This is a string for the test");
		packer.packLong((long) 123456789);
		packer.packDouble((double) 98765.4321);
		packer.packNil();
		packer.flush();
	    } catch (IOException e) {
		System.out.println("error");
	    }
	    byte[] ba = outputStream.toByteArray();
	    var size = outputStream.size();
	    
	    buf = ByteBuffer.allocateDirect(size);
	    buf.put(ba, 0, size);
	    buf.rewind();
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
	}
    }

    MsgPackInputStreamMock msgPackInputStreamMock;

    ResultSetWireMock() {
	msgPackInputStreamMock = new MsgPackInputStreamMock();
    }
    
    public SchemaProtos.RecordMeta recvMeta() throws IOException {
	return ProtosForTest.SchemaProtosChecker.builder().build();
    }

    public MsgPackInputStream getMsgPackInputStream() {
	return msgPackInputStreamMock;
    }

    public void close() throws IOException {
    }


}
