package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.IOException;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.ipc.connection.IpcConnectorImpl;
import com.tsurugidb.tsubakuro.channel.stream.connection.StreamConnectorImpl;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * ConnectorImpl type.
 */
// FIXME: remove superseded impl
public final class ConnectorImpl implements Connector {
    private Connector connector;

    public ConnectorImpl(String name) {
        if (name.contains(":")) {
            String[] elements = name.split(":");

            int port;
            if (elements.length < 2) {
                port = StreamConnectorImpl.DEFAULT_PORT;
            } else {
                if (elements.length > 2) {
                    System.err.println("`:` shoud be one in the connection name");
                }
                try {
                    port = Integer.parseInt(elements[1]);
                } catch (NumberFormatException e) {
                    System.err.println(elements[1] + " is not a number, use default port ("
                            + StreamConnectorImpl.DEFAULT_PORT + ")");
                    port = StreamConnectorImpl.DEFAULT_PORT;
                }
            }
            this.connector = new StreamConnectorImpl(elements[0], port);
        } else {
            this.connector = new IpcConnectorImpl(name);
        }
    }

    @Override
    public FutureResponse<Wire> connect(Credential credential) throws IOException {
        return connector.connect(credential);
    }
}