package com.tsurugidb.tsubakuro.exception;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.tsubakuro.util.ClassCollector;
import com.tsurugidb.tsubakuro.util.DocumentedElement;

/**
 * Collects declared {@link DiagnosticCode} from diagnostic code class definition files in class-path.
 *
 * <p>
 * Locate the diagnostic code class definition file as {@link #PATH_METADATA "META-INF/tsurugidb/diagnostic-code"},
 * and list the target class name on each line in the file (UTF-8).
 * </p>
 * @see DiagnosticCode
 */
public final class DiagnosticCodeCollector {

    /**
     * Path to the diagnostic code class definition file in class-path.
     */
    public static final String PATH_METADATA = "META-INF/tsurugidb/diagnostic-code"; //$NON-NLS-1$

    /**
     * Character set encoding of {@link #PATH_METADATA}.
     */
    public static final Charset ENCODING_METADATA = ClassCollector.ENCODING_METADATA;

    /**
     * Returns a list of registered {@link DiagnosticCode} implementations.
     * @param failOnError {@code true} to raise and error if any exceptions are occurred,
     *      or {@code false} to ignore such the entries and only record logs
     * @return available {@link DiagnosticCode} implementations
     * @throws IOException if failed to load server client definition files
     * @throws IOException if the definitions are not valid when {@code failOnError} is {@code true}
     */
    public static List<Class<? extends DiagnosticCode>> collect(boolean failOnError) throws IOException {
        return collect(DiagnosticCode.class.getClassLoader(), failOnError);
    }

    /**
     * Returns a list of registered {@link DiagnosticCode} implementations.
     * @param loader the class loader to load the subclasses
     * @param failOnError {@code true} to raise and error if any exceptions are occurred,
     *      or {@code false} to ignore such the entries and only record logs
     * @return available {@link DiagnosticCode} implementations
     * @throws IOException if failed to load server client definition files
     * @throws IOException if the definitions are not valid when {@code failOnError} is {@code true}
     */
    public static List<Class<? extends DiagnosticCode>> collect(
            @Nonnull ClassLoader loader,
            boolean failOnError) throws IOException {
        Objects.requireNonNull(loader);
        return ClassCollector.collect(DiagnosticCode.class, loader, PATH_METADATA, failOnError);
    }

    /**
     * Extracts documents about individual diagnostics from the specified {@link DiagnosticCode} class.
     * @param <T> the generics for the target class
     * @param codeClass the target {@link DiagnosticCode} class
     * @return the service message version code, or {@code empty} if it is not defined
     */
    public static <T> List<DocumentedElement<T>> findDocument(@Nonnull Class<T> codeClass) {
        Objects.requireNonNull(codeClass);
        return DocumentedElement.constantsOf(codeClass);
    }

    private DiagnosticCodeCollector() {
        throw new AssertionError();
    }
}
