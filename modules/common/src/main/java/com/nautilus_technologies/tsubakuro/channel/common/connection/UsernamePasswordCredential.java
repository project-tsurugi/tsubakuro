package  com.nautilus_technologies.tsubakuro.channel.common.connection;

import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A credential by user name and password.
 */
public class UsernamePasswordCredential implements Credential {

    private final String name;

    private final String password;

    /**
     * Creates a new instance.
     * @param name the user name
     * @param password the password
     */
    public UsernamePasswordCredential(@Nonnull String name, @Nullable String password) {
        Objects.requireNonNull(name);
        this.name = name;
        this.password = password;
    }

    /**
     * Returns the user name.
     * @return the user name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the password.
     * @return the password
     */
    public Optional<String> getPassword() {
        return Optional.of(password);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "UsernamePasswordCredential(name={0})",
                name);
    }
}
