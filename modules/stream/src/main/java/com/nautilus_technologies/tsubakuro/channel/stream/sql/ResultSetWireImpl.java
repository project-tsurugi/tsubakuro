package com.nautilus_technologies.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.ByteBufferInput;
import com.nautilus_technologies.tsubakuro.channel.common.sql.ResultSetWire;
import com.nautilus_technologies.tsubakuro.channel.stream.StreamWire;
import com.nautilus_technologies.tsubakuro.channel.stream.connection.StreamConnectorImpl;

/**
 * ResultSetWireImpl type.
 */
public class ResultSetWireImpl implements ResultSetWire {
    private StreamWire streamWire;
    private StreamConnectorImpl streamConnector;
    private String sessionName;
    private ByteBufferBackedInput byteBufferBackedInput;
    //    private boolean eor;

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
		if (!streamWire.receive()) {
		    return null;
		}
	    } catch (IOException e) {
		System.err.println(e);
		e.printStackTrace();
		return null;
	    }
	    var buffer = streamWire.getBytes();
	    streamWire.release();
	    if (Objects.isNull(buffer)) {
		return null;
	    }
	    super.reset(ByteBuffer.wrap(buffer));
	    return super.next();
	}
    }

    /**
     * Class constructor, called from FutureResultWireImpl.
     * @param sessionWireHandle the handle of the sessionWire to which the transaction that created this object belongs
     * @param name the name of the ResultSetWireImpl to be created
     */
    public ResultSetWireImpl(StreamConnectorImpl streamConnector, String sessionName) throws IOException {
	this.streamConnector = streamConnector;
	this.sessionName = sessionName;
	this.byteBufferBackedInput = null;
    }

    /**
     * Connect this to the wire specifiec by the name.
     */
    public void connect(String name) throws IOException {
	if (name.length() == 0) {
	    throw new IOException("ResultSet wire name is empty");
	}
	try {
	    streamWire = streamConnector.connect(sessionName + "-" + name).get();
	} catch (Exception e) {
	    throw new IOException(e);
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
		var buffer = streamWire.getBytes();
		streamWire.release();
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
	if (Objects.nonNull(streamWire)) {
	    while (streamWire.receive()) {
		streamWire.release();
	    }
	    streamWire.close();
	    streamWire = null;
	}
	if (Objects.nonNull(byteBufferBackedInput)) {
	    byteBufferBackedInput.close();
	}
    }
}
