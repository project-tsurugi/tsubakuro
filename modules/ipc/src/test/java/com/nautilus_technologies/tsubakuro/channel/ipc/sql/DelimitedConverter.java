package com.nautilus_technologies.tsubakuro.channel.ipc.sql;

import java.io.IOException;
import java.io.ByteArrayOutputStream;

import com.tsurugidb.tateyama.proto.SqlRequest;
import com.tsurugidb.tateyama.proto.SqlResponse;

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
