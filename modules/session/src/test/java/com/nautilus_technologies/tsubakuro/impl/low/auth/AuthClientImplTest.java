package com.nautilus_technologies.tsubakuro.impl.low.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tateyama.proto.AuthRequest;
import com.nautilus_technologies.tsubakuro.low.auth.AuthInfo;
import com.nautilus_technologies.tsubakuro.low.auth.AuthService;
import com.nautilus_technologies.tsubakuro.channel.common.connection.RememberMeCredential;
import com.nautilus_technologies.tsubakuro.util.FutureResponse;

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
