package com.tsurugidb.tsubakuro.channel.stream.sql;

import java.io.IOException;
import java.io.ByteArrayOutputStream;

import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.sql.proto.SqlResponse;

public final class DelimitedConverter {
    private DelimitedConverter() {
    }
    
    public static byte[] toByteArray(SqlRequest.Request request) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            request.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
    public static byte[] toByteArray(SqlResponse.Response response) throws IOException {
        try (var buffer = new ByteArrayOutputStream()) {
            response.writeDelimitedTo(buffer);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOException(e);
        }
    }
}
