package com.tsurugidb.tsubakuro.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects declared {@link ServiceClient} from service client definition files in class-path.
 *
 * <p>
 * Locate the service definition file as {@link #PATH_METADATA "META-INF/tsurugidb/clients"},
 * and list the target class name on each line in the file (UTF-8).
 * </p>
 * @see ServiceClient
 */
public final class ServiceClientCollector {

    static final Logger LOG = LoggerFactory.getLogger(ServiceClientCollector.class);

    /**
     * Path to the service client definition file in class-path.
     */
    public static final String PATH_METADATA = "META-INF/tsurugidb/clients"; //$NON-NLS-1$

    /**
     * Character set encoding of {@link #PATH_METADATA}.
     */
    public static final Charset ENCODING_METADATA = StandardCharsets.UTF_8;

    private static final char COMMENT_START = '#';

    /**
     * Returns a list of available ServiceClient subclasses.
     * @param failOnError {@code true} to raise and error if any exceptions are occurred,
     *      or {@code false} to ignore such the entries and only record logs
     * @return available ServiceClient subclasses
     * @throws IOException if failed to load server client definition files
     * @throws IOException if the definitions are not valid when {@code failOnError} is {@code true}
     */
    public static List<? extends Class<? extends ServiceClient>> collect(boolean failOnError) throws IOException {
        return collect(ServiceClient.class.getClassLoader(), failOnError);
    }

    /**
     * Returns a list of available ServiceClient subclasses.
     * @param loader the class loader to load the subclasses
     * @param failOnError {@code true} to raise and error if any exceptions are occurred,
     *      or {@code false} to ignore such the entries and only record logs
     * @return available ServiceClient subclasses
     * @throws IOException if failed to load server client definition files
     * @throws IOException if the definitions are not valid when {@code failOnError} is {@code true}
     */
    public static List<? extends Class<? extends ServiceClient>> collect(
            @Nonnull ClassLoader loader,
            boolean failOnError) throws IOException {
        Objects.requireNonNull(loader);
        List<Class<? extends ServiceClient>> results = new ArrayList<>();
        LOG.debug("loading {}", PATH_METADATA); //$NON-NLS-1$
        var resources = loader.getResources(PATH_METADATA);
        while (resources.hasMoreElements()) {
            var element = resources.nextElement();
            LOG.debug("found service client definition: {}", element); //$NON-NLS-1$

            var found = collectFromUrl(element, loader, failOnError);
            results.addAll(found);
        }
        return results;
    }

    private static List<? extends Class<? extends ServiceClient>> collectFromUrl(
            URL url, ClassLoader loader, boolean failOnError) throws IOException {
        List<Class<? extends ServiceClient>> results = new ArrayList<>();
        try (var in = url.openStream();
                var isr = new InputStreamReader(in, ENCODING_METADATA);
                var reader = new BufferedReader(isr)) {
            while (true) {
                var line = reader.readLine();
                if (line == null) {
                    break;
                }
                // strip comments
                var commentAt = line.indexOf(COMMENT_START);
                if (commentAt >= 0) {
                    line = line.substring(0, commentAt);
                }
                var className = line.trim();
                // skip empty lines
                if (className.isEmpty()) {
                    continue;
                }

                LOG.debug("loading ServiceClient class: {}", className); //$NON-NLS-1$
                try {
                    var clazz = loader.loadClass(className);
                    results.add(clazz.asSubclass(ServiceClient.class));
                } catch (ClassNotFoundException e) {
                    var message = MessageFormat.format(
                            "cannot load ServiceClient: {0} ({1})",
                            className,
                            url);
                    if (failOnError) {
                        throw new IOException(message, e);
                    }
                    LOG.warn(message);
                } catch (ClassCastException e) {
                    var message = MessageFormat.format(
                            "ServiceClient is not valid: {0} ({1})",
                            className,
                            url);
                    if (failOnError) {
                        throw new IOException(message, e);
                    }
                    LOG.warn(message);
                }
            }
        } catch (IOException e) {
            if (failOnError) {
                throw e;
            }
            LOG.warn(MessageFormat.format(
                    "error occurred while loading service definition file: {0}",
                    url));
        }
        return results;
    }

    /**
     * Extracts service message version code from the {@link ServiceClient} interface.
     *
     * <p>
     * The service message code is is string format as follows:
     *
     * <code>
     * "&lt;{@linkplain ServiceMessageVersion#service() service
     * }&gt;-&lt;{@linkplain ServiceMessageVersion#major() major
     * }&gt;-&lt;{@linkplain ServiceMessageVersion#service() minor}&gt;"
     * </code>.
     * </p>
     *
     * @param clientClass the target {@link ServiceClient} interface
     * @return the service message version code, or {@code empty} if it is not defined
     */
    public static Optional<String> findServiceMessageVersion(@Nonnull Class<? extends ServiceClient> clientClass) {
        Objects.requireNonNull(clientClass);
        var annotation = clientClass.getAnnotation(ServiceMessageVersion.class);
        return Optional.ofNullable(annotation)
                .map(ServiceClientCollector::toServiceMessageVersionCode);
    }

    private static String toServiceMessageVersionCode(ServiceMessageVersion annotation) {
        return String.format(
                "%s-%d.%d", // //$NON-NLS-1$
                annotation.service(),
                annotation.major(),
                annotation.minor());
    }

    private ServiceClientCollector() {
        throw new AssertionError();
    }
}
