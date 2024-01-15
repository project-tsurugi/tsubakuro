package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.text.MessageFormat;

import javax.annotation.Nullable;

/**
 * A credential by user name and password.
 */
public final class ClientInformation {

    private final String connectionLabel;

    private final String applicationName;

    private final String userName;

    /**
     * Creates a new instance without information.
     */
    public ClientInformation() {
        this.connectionLabel = null;
        this.applicationName = null;
        this.userName = null;
    }

    /**
     * Creates a new instance.
     * @param connectionLabel the label
     * @param applicationName the application name
     * @param userName the application name
     */
    public ClientInformation(@Nullable String connectionLabel, @Nullable String applicationName, @Nullable String userName) {
        this.connectionLabel = connectionLabel;
        this.applicationName = applicationName;
        this.userName = userName;
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
     * Get the user name.
     * @return the user name, null if user name has not been set.
     */
    public String getUserName() {
        return userName;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ClientInformation(connectionLabel={0}, applicationName={1}, userName={2})",
                checkNull(connectionLabel), checkNull(applicationName), checkNull(userName));
    }
    private String checkNull(String string) {
        return (string != null) ? string : "";
    }
}
