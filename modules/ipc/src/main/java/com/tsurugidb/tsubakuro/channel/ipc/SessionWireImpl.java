package com.tsurugidb.tsubakuro.channel.ipc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.framework.proto.FrameworkRequest;
import com.tsurugidb.tsubakuro.channel.common.connection.sql.ResultSetWire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.ResponseWireHandle;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

/**
 * SessionWireImpl type.
 */
public final class SessionWireImpl implements Wire {

    static final Logger LOG = LoggerFactory.getLogger(SessionWireImpl.class);

    private IpcWire ipcWire;
    private final String dbName;
    private final long sessionID;
    private final ResponseBox responseBox;

    /**
     * Class constructor, called from IpcConnectorImpl that is a connector to the SQL server.
     * @param dbName the name of the SQL server to which this SessionWireImpl is to be connected
     * @param sessionID the id of this session obtained by the connector requesting a connection to the SQL server
     * @throws IOException error occurred in openNative()
     */
    public SessionWireImpl(String dbName, long sessionID) throws IOException {
        ipcWire = new IpcWire(dbName + "-" + String.valueOf(sessionID));
        this.dbName = dbName;
        this.sessionID = sessionID;
        this.responseBox = ipcWire.getResponseBox();
        ipcWire.start();
        LOG.trace("begin Session via stream, id = {}", sessionID);
    }

    /**
     * Send a Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in byte[]
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in ByteBuffer variant of send()
     */
    @Override
    public FutureResponse<? extends Response> send(int serviceId, @Nonnull byte[] payload) throws IOException {
        if (Objects.isNull(ipcWire)) {
            throw new IOException("already closed");
        }
        var header = FrameworkRequest.Header.newBuilder().setMessageVersion(1).setServiceId(serviceId).setSessionId(sessionID).build();
        var response = responseBox.register(toDelimitedByteArray(header), payload);
        return FutureResponse.wrap(Owner.of(response));
    }

    /**
     * Send a Request to the server via the native wire.
     * @param serviceId the destination service ID
     * @param payload the Request message in ByteBuffer
     * @return a Future response message corresponding the request
     * @throws IOException error occurred in responseBox.register()
     */
    @Override
    public  FutureResponse<? extends Response> send(int serviceId, @Nonnull ByteBuffer payload) throws IOException {
        return send(serviceId, payload.array());
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle) throws IOException {
//        if (wireHandle == 0) {
//            throw new IOException("already closed");
//        }
//        var responseWireHandle = ((ResponseWireHandleImpl) handle).getHandle();
//        var byteBuffer = receiveNative(responseWireHandle);

//        byte[] ba = new byte[byteBuffer.capacity()];
//        byteBuffer.get(ba);
//        release(handle);
//        var newBuffer = ByteBuffer.wrap(ba);
//        FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(newBuffer));
//        return newBuffer;
        return null;
    }

    @Override
    public ByteBuffer response(ResponseWireHandle handle, long timeout, TimeUnit unit) throws TimeoutException, IOException {
//        if (wireHandle == 0) {
//            throw new IOException("already closed");
//        }
//        var responseWireHandle = ((ResponseWireHandleImpl) handle).getHandle();
//        var timeoutNano = unit.toNanos(timeout);
//        if (timeoutNano == Long.MIN_VALUE) {
//            throw new IOException("timeout duration overflow");
//        }
//        var byteBuffer = receiveNative(responseWireHandle, timeoutNano);

//        byte[] ba = new byte[byteBuffer.capacity()];
//        byteBuffer.get(ba);
//        release(handle);
//        var newBuffer = ByteBuffer.wrap(ba);
//        FrameworkResponse.Header.parseDelimitedFrom(new ByteBufferInputStream(newBuffer));
//        return newBuffer;
        return null;
    }

    /**
     * release the message in the response box
     * @param handle the handle to the response box
    */
//    private void release(ResponseWireHandle handle) throws IOException {
//        releaseNative(((ResponseWireHandleImpl) handle).getHandle());

//        synchronized (this) {
//            var entry = queue.peek();
//            if (Objects.nonNull(entry)) {
//                var nextHandle = getResponseHandleNative(wireHandle);
//                if (nextHandle != 0) {
//                    entry.getResponse().setResponseHandle(new ResponseWireHandleImpl(nextHandle));

//                    var header = HEADER_BUILDER.setServiceId(entry.serviceId()).setSessionId(sessionID).build();
//                    sendNative(wireHandle, toDelimitedByteArray(header));
//                    sendNative(wireHandle, entry.getRequest());
//                    flushNative(wireHandle);
//                    queue.poll();
//                    LOG.trace("send {}, handle = {}", entry.getRequest(), handle);
//                }
//            }
//        }
//    }

    /**
     * Create a ResultSetWire without a name, meaning that this wire is not connected
     * @return ResultSetWireImpl
    */
    @Override
    public ResultSetWire createResultSetWire() throws IOException {
        if (Objects.isNull(ipcWire)) {
            throw new IOException("already closed");
        }
        return ipcWire.createResultSetWire();
    }

    /**
     * Close the wire
     */
    @Override
    public void close() throws IOException {
        if (Objects.nonNull(ipcWire)) {
            ipcWire.close();
            ipcWire = null;
        }
    }

    byte[] toDelimitedByteArray(FrameworkRequest.Header request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public String getDbName() {
        return dbName;
    }
}
