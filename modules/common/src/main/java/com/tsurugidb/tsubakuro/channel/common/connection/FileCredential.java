package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Credentials information from credential files.
 *
 * <p>
 * Each credential file is JSON formatted and has the following fields:
 * </p>
 * <ul>
 * <li> {@code "user"} - {@link #getEncryptedName() encrypted user name} </li>
 * <li> {@code "user"} - {@link #getEncryptedPassword() encrypted password} </li>
 * </ul>
 *
 * Each field value will be encrypted using public key file.
 */
public class FileCredential implements Credential {

    private static final Logger LOG = LoggerFactory.getLogger(FileCredential.class);

    /**
     * The encrypted user name field name in credential file.
     */
    public static final String KEY_USER = "user"; //$NON-NLS-1$

    /**
     * The encrypted password field name in credential file.
     */
    public static final String KEY_PASSWORD = "password"; //$NON-NLS-1$

    /**
     * The default credential path.
     */
    public static final Optional<Path> DEFAULT_CREDENTIAL_PATH = Optional.ofNullable(System.getProperty("user.home")) //$NON-NLS-1$
            .filter(it -> !it.isBlank())
            .map(Path::of)
            .map(it -> it.resolve(".tsurugidb/credentials.json")); //$NON-NLS-1$

    private static final JsonFactory JSON = new JsonFactoryBuilder()
            .build();

    private final String encryptedName;

    private final String encryptedPassword;

    /**
     * Creates a new instance.
     * @param encryptedName the encrypted user name
     * @param encryptedPassword the encrypted password
     * @see #load(Path)
     */
    public FileCredential(@Nonnull String encryptedName, @Nonnull String encryptedPassword) {
        Objects.requireNonNull(encryptedName);
        Objects.requireNonNull(encryptedPassword);
        this.encryptedName = encryptedName;
        this.encryptedPassword = encryptedPassword;
    }

    /**
     * Returns the encrypted user name.
     * @return the encrypted user name.
     */
    public String getEncryptedName() {
        return encryptedName;
    }

    /**
     * Returns the encrypted password.
     * @return the encrypted password
     */
    public String getEncryptedPassword() {
        return encryptedPassword;
    }

    @Override
    public String toString() {
        return "FileCredential()";
    }

    /**
     * Extracts credential information from the file.
     * @param file the input credential file
     * @return the loaded credential information
     * @throws IOException if I/O error was occurred while loading the credential file
     * @throws FileNotFoundException if the input credential file is not found
     * @see #DEFAULT_CREDENTIAL_PATH
     * @see #dump(Path)
     */
    public static FileCredential load(@Nonnull Path file) throws IOException {
        Objects.requireNonNull(file);
        LOG.trace("loading credential file: {}", file); //$NON-NLS-1$
        if (!Files.isRegularFile(file)) {
            throw new FileNotFoundException(MessageFormat.format(
                    "credential file is not found: {0}",
                    file));
        }
        var mapper = new ObjectMapper(JSON);
        var tree = mapper.readTree(file.toFile());

        var user = extractField(tree, KEY_USER, file);
        var password = extractField(tree, KEY_PASSWORD, file);

        return new FileCredential(user, password);
    }

    private static String extractField(JsonNode node, String key, Path file) throws IOException {
        assert node != null;
        assert key != null;
        assert file != null;
        var field = node.get(key);
        if (field == null || !field.isTextual()) {
            throw new IOException(MessageFormat.format(
                    "missing field ''{0}'' in credential file: {1}",
                    key,
                    file));
        }
        return field.asText();
    }

    /**
     * Writes a credential file into the given path.
     * @param file the target path
     * @throws IOException if I/O error was occurred while writing the destination path
     * @see #DEFAULT_CREDENTIAL_PATH
     * @see #load(Path)
     */
    public void dump(@Nonnull Path file) throws IOException {
        Objects.requireNonNull(file);
        LOG.trace("writing credential file: {}", file); //$NON-NLS-1$

        try (var writer = JSON.createGenerator(file.toFile(), JsonEncoding.UTF8)) {
            writer.writeStartObject();
            writer.writeFieldName(KEY_USER);
            writer.writeString(getEncryptedName());
            writer.writeFieldName(KEY_PASSWORD);
            writer.writeString(getEncryptedPassword());
            writer.writeEndObject();
        }
    }
}
