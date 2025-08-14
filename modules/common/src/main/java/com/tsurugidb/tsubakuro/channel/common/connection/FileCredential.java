/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package  com.tsurugidb.tsubakuro.channel.common.connection;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credentials information from credential files.
 *
 */
public class FileCredential implements Credential {

    private static final Logger LOG = LoggerFactory.getLogger(FileCredential.class);

    /**
     * The default credential path.
     */
    public static final Optional<Path> DEFAULT_CREDENTIAL_PATH = Optional.ofNullable(System.getProperty("user.home")) //$NON-NLS-1$
            .filter(it -> !it.isBlank())
            .map(Path::of)
            .map(it -> it.resolve(".tsurugidb/credentials.key")); //$NON-NLS-1$

    private final String encrypted;

    private final List<String> comments;

    /**
     * @param encrypted the encrypted credential string
     * @param comments the optional comment lines for the credential
     */
    public FileCredential(@Nonnull String encrypted, @Nonnull List<String> comments) {
        Objects.requireNonNull(encrypted);
        Objects.requireNonNull(comments);
        this.encrypted = encrypted;
        this.comments = List.copyOf(comments);
    }

    /**
     * Returns the encrypted credential string.
     * @return the encrypted credential string
     */
    public String getEncrypted() {
        return encrypted;
    }

    /**
     * Returns the comment lines for the credential.
     * @return the comment lines, or empty list if no comments are specified
     */
    public List<String> getComments() {
        return comments;
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
        var lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new FileNotFoundException(MessageFormat.format(
                    "credential file is empty: {0}",
                    file));
        }
        if (lines.size() == 1) {
            return new FileCredential(lines.get(0), List.of());
        }
        return new FileCredential(lines.get(0), lines.subList(1, lines.size()));
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

        FileWriter fileWriter = new FileWriter(file.toString(), StandardCharsets.UTF_8, false);
        try (var writer = fileWriter) {
            writer.write(encrypted + "\n"); //$NON-NLS-1$
            for (String comment : comments) {
                writer.write(comment + "\n"); //$NON-NLS-1$
            }
        } catch (IOException e) {
            LOG.error("Failed to write credential file: {}", file, e); //$NON-NLS-1$
            throw e;
        }
    }

    @Override
    public int hashCode() {
        // hashCode is spec'd to compare only encrypted
        return Objects.hash(encrypted);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FileCredential other = (FileCredential) obj;
        return encrypted.equals(other.encrypted);
    }

    @Override
    public String toString() {
        return String.format("FileCredential{commentsCount=%d}", comments.size());
    }
}