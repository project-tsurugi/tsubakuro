package com.tsurugidb.tsubakuro.channel.ipc.sql;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;

public final class DelimitedConverter {
    private DelimitedConverter() {
    }

    public static byte[] toByteArray(SqlRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }
    public static byte[] toByteArray(SqlResponse.Response response) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            response.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        }
    }
}
