package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * An authenticated token from authority center.
 */
public class RememberMeCredential implements Credential {

    private final String token;

    /**
     * Creates a new instance.
     * @param token the authenticated token string
     */
    public RememberMeCredential(@Nonnull String token) {
        Objects.requireNonNull(token);
        this.token = token;
    }

    /**
     * Returns the token text.
     * @return the token text
     */
    public String getToken() {
        return token;
    }

    @Override
    public String toString() {
        return "RememberMeCredential()";
    }
}
