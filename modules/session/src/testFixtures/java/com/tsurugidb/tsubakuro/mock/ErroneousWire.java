package com.tsurugidb.tsubakuro.mock;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Response;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.WireFactory;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;
import com.tsurugidb.tsubakuro.exception.CoreServiceException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Owner;

/**
 * Mock {@link Wire} which always raises {@link CoreServiceException}.
 */
public class ErroneousWire implements Wire {

    /**
     * The scheme name of this wire.
     */
    public static final String SCHEME = "erroneous"; //$NON-NLS-1$

    @Override
    public FutureResponse<Response> send(int serviceId, ByteBuffer payload) {
        return FutureResponse.raises(new CoreServiceException(CoreServiceCode.UNSUPPORTED_OPERATION));
    }

    @Override
    public void close() {
        // do nothing
    }

    /**
     * Provides {@link ErroneousWire}.
     */
    public static class Factory implements WireFactory {

        @Override
        public boolean accepts(URI endpoint) {
            return Objects.equals(endpoint.getScheme(), SCHEME);
        }

        @Override
        public FutureResponse<ErroneousWire> create(URI endpoint, Credential credential) {
            return FutureResponse.wrap(Owner.of(new ErroneousWire()));
        }
    }
}
