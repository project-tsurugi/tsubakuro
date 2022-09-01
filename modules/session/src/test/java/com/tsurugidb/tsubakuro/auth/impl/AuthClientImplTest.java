package com.tsurugidb.tsubakuro.auth.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tateyama.proto.AuthRequest;
import com.tsurugidb.tsubakuro.auth.AuthInfo;
import com.tsurugidb.tsubakuro.auth.AuthService;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.util.FutureResponse;

class AuthClientImplTest {

    @Test
    void getAuthInfo() throws Exception {
        var client = new AuthClientImpl(new AuthService() {
            @Override
            public FutureResponse<AuthInfo> send(AuthRequest.AuthInfo request) throws IOException {
                return FutureResponse.returns(new AuthInfo("A", new RememberMeCredential("T")));
            }
        });
        var info = client.getAuthInfo().await();
        assertEquals("A", info.getName());
        assertEquals("T", info.getToken().getToken());
    }
}
