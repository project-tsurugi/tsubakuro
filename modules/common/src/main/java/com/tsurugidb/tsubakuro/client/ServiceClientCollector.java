package com.tsurugidb.tsubakuro.client;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.util.ClassCollector;

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

    /**
     * Path to the service client definition file in class-path.
     */
    public static final String PATH_METADATA = "META-INF/tsurugidb/clients"; //$NON-NLS-1$

    /**
     * Character set encoding of {@link #PATH_METADATA}.
     */
    public static final Charset ENCODING_METADATA = ClassCollector.ENCODING_METADATA;

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
        return ClassCollector.collect(ServiceClient.class, loader, PATH_METADATA, failOnError);
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
