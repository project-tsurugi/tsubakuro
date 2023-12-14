package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.text.MessageFormat;
import java.util.Optional;

/**
 * A credential by user name and password.
 */
public class ClientInformation {

    private String label;

    private String applicationName = "tsubakuro";

    private final String connectionInformation;

    /**
     * Creates a new instance.
     */
    public ClientInformation() {
        this.connectionInformation = Long.toString(ProcessHandle.current().pid());
    }

    /**
     * Returns the label.
     * @return the label.
     */
    public Optional<String> getLabel() {
        return Optional.of(label); 
    }

    /**
     * Returns the application name.
     * @return the application name.
     */
    public Optional<String> getApplicationName() {
        return Optional.of(applicationName);
    }

        /**
     * Returns the connection information.
     * @return the connection information.
     */
    public String getConnectionInformation() {
        return connectionInformation;
    }

    /**
     * Set label.
     * @param labelString the label
     */
    public void label(String labelString) {
        this.label = labelString;
    }

    /**
     * Set application name.
     * @param applicationNameString the application name
     */
    public void applicationName(String applicationNameString) {
        this.applicationName = applicationNameString;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ClientInformation(label={0}, applicationName={1}, connectionInformation={2}))",
                label, applicationName, connectionInformation);
    }
}
