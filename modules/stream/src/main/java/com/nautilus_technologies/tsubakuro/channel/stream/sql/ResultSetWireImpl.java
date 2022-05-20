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

    private class ByteBufferBackedInput extends ByteBufferInput {
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
     * @param streamWire the stream object of the sessionWire
     */
    public ResultSetWireImpl(StreamWire streamWire) {
    this.streamWire = streamWire;
    this.resultSetBox = streamWire.getResultSetBox();
    this.byteBufferBackedInput = null;
    }

    /**
     * Connect this to the wire specifiec by the name.
     * @param name the result set name specified by the SQL server.
     * @throws IOException connection error
     */
    public void connect(String name) throws IOException {
    if (name.length() == 0) {
        throw new IOException("ResultSet wire name is empty");
    }
    slot = resultSetBox.hello(name);
    }

    /**
     * Provides the Input to retrieve the received data.
     */
    public ByteBufferInput getByteBufferBackedInput() {
    if (Objects.isNull(byteBufferBackedInput)) {
        try {
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
    // When multiple writer is implemented, this becomes necessary.
    return true;
    }

    /**
     * Close the wire
     */
    public void close() throws IOException {
    while (true) {
        var entry = resultSetBox.receive(slot);
        if (Objects.isNull(entry.getPayload())) {
        break;
        }
    }
    streamWire.sendResutSetByeOk(slot);
    if (Objects.nonNull(byteBufferBackedInput)) {
        byteBufferBackedInput.close();
    }
    }
}
