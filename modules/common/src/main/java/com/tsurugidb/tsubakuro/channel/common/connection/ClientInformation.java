package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.text.MessageFormat;

/**
 * A credential by user name and password.
 */
public class ClientInformation {

    private String connectionLabel;

    private String applicationName;

    private String userName;

    private String connectionInformation;  // used by IPC only

    private long maxResultSets;  // used by Stream

    /**
     * Creates a new instance.
     */
    public ClientInformation() {
    }

    /**
     * Get the connection label.
     * @return the connection label.
     */
    public String connectionLabel() {
        if (connectionLabel != null) {
            return connectionLabel;
        }
        return "";
    }

    /**
     * Get the application name.
     * @return the application name.
     */
    public String applicationName() {
        if (applicationName != null) {
            return applicationName;
        }
        return "";
    }

    /**
     * Get the user name.
     * @return the user name.
     */
    public String userName() {
        if (userName != null) {
            return userName;
        }
        return "";
    }

    /**
     * Get the connection information.
     * @return the connection information.
     */
    public String connectionInformation() {
        if (connectionInformation != null) {
            return connectionInformation;
        }
        return "";
    }

    /**
     * Get the maximumConcurrentResultSets.
     * @return the maximumConcurrentResultSets.
     */
    public long maximumConcurrentResultSets() {
        return maxResultSets;
    }

    /**
     * Set label.
     * @param labelString the label
     */
    public void connectionlabel(String labelString) {
        this.connectionLabel = labelString;
    }

    /**
     * Set application name.
     * @param applicationNameString the application name
     */
    public void applicationName(String applicationNameString) {
        this.applicationName = applicationNameString;
    }

    /**
     * Set user name.
     * @param userNameString the application name
     */
    public void userName(String userNameString) {
        this.userName = userNameString;
    }

    /**
     * Set connectionInformation name.
     * @param connectionInformationString the connectionInformation
     */
    public void connectionInformation(String connectionInformationString) {
        this.connectionInformation = connectionInformationString;
    }

    /**
     * Set maximumConcurrentResultSets.
     * @param maximumConcurrentResultSets the number of maximumConcurrentResultSets
     */
    public void maximumConcurrentResultSets(long maximumConcurrentResultSets) {
        this.maxResultSets = maximumConcurrentResultSets;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ClientInformation(connectionLabel={0}, applicationName={1}, userName={2}, connectionInformation={3}, maximumConcurrentResultSets={4})",
                connectionLabel, applicationName, userName, connectionInformation, maxResultSets);
    }
}
