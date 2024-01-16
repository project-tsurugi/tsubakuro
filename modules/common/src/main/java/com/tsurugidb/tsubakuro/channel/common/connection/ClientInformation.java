package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.text.MessageFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A client information intened to be used in handshake.
 */
public final class ClientInformation {

    private final String connectionLabel;

    private final String applicationName;

    private final Credential credential;

    /**
     * Creates a new instance without information.
     */
    public ClientInformation() {
        this.connectionLabel = null;
        this.applicationName = null;
        this.credential = NullCredential.INSTANCE;
    }

    /**
     * Creates a new instance.
     * @param connectionLabel the label.
     * @param applicationName the application name.
     * @param credential the connection credential.
     */
    public ClientInformation(@Nullable String connectionLabel, @Nullable String applicationName, @Nonnull Credential credential) {
        this.connectionLabel = connectionLabel;
        this.applicationName = applicationName;
        this.credential = credential;
    }

    /**
     * Get the connection label.
     * @return the connection label, null if connection label has not been set.
     */
    public String getConnectionLabel() {
        return connectionLabel;
    }

    /**
     * Get the application name.
     * @return the application name, null if application name has not been set.
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Get the credential.
     * @return the connection credential.
     */
    public Credential getCredential() {
        return credential;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ClientInformation(connectionLabel={0}, applicationName={1}, credential={2})",
                checkNull(connectionLabel), checkNull(applicationName), credential.toString());
    }
    private String checkNull(String string) {
        return (string != null) ? string : "";
    }

}
