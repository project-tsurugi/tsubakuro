package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.ByteBufferInput;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private StreamWire streamWire;
    private ResultSetBox resultSetBox;
    private int slot;
    private ByteBufferBackedInput byteBufferBackedInput;

    class ByteBufferBackedInput extends ByteBufferInput {
	ByteBufferBackedInput(ByteBuffer byteBuffer) {
	    super(byteBuffer);
	}

	public MessageBuffer next() {
	    var rv = super.next();
	    if (Objects.nonNull(rv)) {
		return rv;
	    }
	    try {
		var receivedData = resultSetBox.receive(slot);
		var buffer = receivedData.getPayload();
		if (Objects.isNull(buffer)) {
		    return null;
		}
		super.reset(ByteBuffer.wrap(buffer));
	    } catch (IOException e) {
		System.err.println(e);
		e.printStackTrace();
		return null;
	    }
	    return super.next();
	}
    }

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param sessionWireHandle the handle of the sessionWire to which the transaction that created this object belongs
     * @param name the name of the ResultSetWireImpl to be created
     */
    public ResultSetWireImpl(StreamWire streamWire) throws IOException {
	this.streamWire = streamWire;
	this.resultSetBox = streamWire.getResultSetBox();
	this.byteBufferBackedInput = null;
    }

    /**
     * Connect this to the wire specifiec by the name.
     */
    public void connect(String name) throws IOException {
	if (name.length() == 0) {
	    throw new IOException("ResultSet wire name is empty");
	}
	slot = resultSetBox.lookFor();
	streamWire.hello(name, slot);
	var response = resultSetBox.receive(slot);
	if (response.getInfo() != StreamWire.RESPONSE_RESULT_SET_HELLO_OK) {
	    throw new IOException("ResultSetWire connect error: " + name);
	}
    }

    /**
     * Provides the Input to retrieve the received data.
     */
    public ByteBufferInput getByteBufferBackedInput() {
	if (Objects.isNull(byteBufferBackedInput)) {
	    try {
		if (!streamWire.receive()) {
		    return null;
		}
		var receivedData = resultSetBox.receive(slot);
		var buffer = receivedData.getPayload();
		if (Objects.isNull(buffer)) {
		    return null;
		}
		byteBufferBackedInput = new ByteBufferBackedInput(ByteBuffer.wrap(buffer));
	    } catch (IOException e) {
		System.err.println(e);
		e.printStackTrace();
		return null;
	    }
	}
	return byteBufferBackedInput;
    }

    public boolean disposeUsedData(long length) throws IOException {
	// FIXME
	return true;
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
	// Do not close the streamWire
	if (Objects.nonNull(byteBufferBackedInput)) {
	    byteBufferBackedInput.close();
	}
    }
}
