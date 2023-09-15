package com.tsurugidb.tsubakuro.channel.ipc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads native library.
 */
public final class NativeLibrary {

    static final Logger LOG = LoggerFactory.getLogger(NativeLibrary.class);

    static final String LIBRARY_NAME = "tsubakuro"; //$NON-NLS-1$

    static final String KEY_PROPERTY_NATIVE_LIBRARY = "com.tsurugidb.tsubakuro.jnilib"; //$NON-NLS-1$

    static final String KEY_ENVIRONMENT_TSURUGI_HOME = "TSURUGI_HOME"; //$NON-NLS-1$

    static final String PATH_NATIVE_LIBRARY_DIR = "lib"; //$NON-NLS-1$

    private static final Path PATH_NATIVE_LIBRARY_PROPERTY =
            Optional.ofNullable(System.getProperty(KEY_PROPERTY_NATIVE_LIBRARY))
                    .map(String::strip)
                    .filter(it -> !it.isEmpty())
                    .map(Path::of)
                    .orElse(null);

    private static final Path PATH_NATIVE_LIBRARY_ENV =
            Optional.ofNullable(System.getenv(KEY_ENVIRONMENT_TSURUGI_HOME))
                    .map(String::strip)
                    .filter(it -> !it.isEmpty())
                    .map(Path::of)
                    .map(it -> it.resolve(PATH_NATIVE_LIBRARY_DIR))
                    .map(it -> it.resolve(System.mapLibraryName(LIBRARY_NAME)))
                    .orElse(null);

    /**
     * The property name whether or not verify versions between Java library and JNI library.
     */
    public static final String KEY_PROPERTY_VERIFY_VERSION = "com.tsurugidb.tsubakuro.jniverify"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_PROPERTY_VERIFY_VERSION}.
     */
    public static final boolean DEFAULT_VERIFY_VERSION = false;

    private static final boolean REQUIRE_VERIFY_VERSION =
            Optional.ofNullable(System.getProperty(KEY_PROPERTY_VERIFY_VERSION))
                    .map(String::strip)
                    .filter(it -> !it.isEmpty())
                    .map(Boolean::valueOf)
                    .orElse(DEFAULT_VERIFY_VERSION);

    private static final String PATH_VERSION_FILE = "/META-INF/tsurugidb/tsubakuro-ipc.properties"; //$NON-NLS-1$

    private static final String ATTRIBUTE_LIBRARY_VERSION = "Build-Revision"; //$NON-NLS-1$

    private static final AtomicBoolean VERSION_VERIFIED = new AtomicBoolean(false);

    private static final AtomicReference<Optional<String>> VERSION_JAVA = new AtomicReference<>();

    private static final AtomicReference<Optional<String>> VERSION_NATIVE = new AtomicReference<>();

    static {
        LOG.trace("loading native library: {}", LIBRARY_NAME); //$NON-NLS-1$
        if (PATH_NATIVE_LIBRARY_PROPERTY != null) {
            LOG.debug("loading native library by property: {}", PATH_NATIVE_LIBRARY_PROPERTY); //$NON-NLS-1$
            System.load(PATH_NATIVE_LIBRARY_PROPERTY.toAbsolutePath().toString());
        } else if (PATH_NATIVE_LIBRARY_ENV != null) {
            LOG.debug("loading native library by {}: {}", KEY_ENVIRONMENT_TSURUGI_HOME, PATH_NATIVE_LIBRARY_ENV); //$NON-NLS-1$
            System.load(PATH_NATIVE_LIBRARY_ENV.toAbsolutePath().toString());
        } else {
            LOG.debug("loading native library from system path: {}", System.mapLibraryName(LIBRARY_NAME)); //$NON-NLS-1$
            System.loadLibrary(LIBRARY_NAME);
        }
    }

    /**
     * Loads the native library.
     * @throws IllegalStateException if native library version is inconsistent
     */
    public static void load() {
        LOG.trace("native library requested");
        if (VERSION_VERIFIED.get()) {
            return;
        }
        if (!REQUIRE_VERIFY_VERSION) {
            LOG.debug("Java library - JNI library version verification is not required"); //$NON-NLS-1$
            return;
        }
        var javaVersion = getJavaLibraryVersion();
        var nativeVersion = getNativeLibraryVersion();
        LOG.debug("verifying Java library ({}) - JNI library version ({})", //$NON-NLS-1$
                javaVersion,
                nativeVersion);

        if (javaVersion.isEmpty()) {
            LOG.debug("Java library version is not detected"); //$NON-NLS-1$
            return;
        }
        if (nativeVersion.isEmpty()) {
            LOG.debug("Native library version is not detected"); //$NON-NLS-1$
            return;
        }
        if (!Objects.equals(javaVersion.get(), nativeVersion.get())) {
            throw new IllegalStateException(MessageFormat.format(
                    "invalid native library version: \"{0}\" (required \"{1}\"): " //$NON-NLS-1$
                    + "to disable this feature, please put option -D{2}=false", //$NON-NLS-1$
                    nativeVersion.get(),
                    javaVersion.get(),
                    KEY_PROPERTY_VERIFY_VERSION));
        }
        VERSION_VERIFIED.set(true);
    }

    /**
     * Returns the Java library version string.
     * @return the Java library version string, or empty if it is not declared
     */
    public static Optional<String> getJavaLibraryVersion() {
        var cached = VERSION_JAVA.get();
        if (cached != null) {
            return cached;
        }
        LOG.trace("retrieving Java library version"); //$NON-NLS-1$
        try {
            Optional<String> retrieved = getJavaLibraryVersion0();
            LOG.debug("Java library version: {}", retrieved); //$NON-NLS-1$
            VERSION_JAVA.compareAndSet(null, retrieved);
            return retrieved;
        } catch (FileNotFoundException e) {
            LOG.info("Java library version info is not found (may not be official build)", e);
            VERSION_JAVA.compareAndSet(null, Optional.empty());
        } catch (IOException e) {
            LOG.warn("error occurred while retrieving Java library version info", e);
            VERSION_JAVA.compareAndSet(null, Optional.empty());
        }
        return Optional.empty();
    }

    private static Optional<String> getJavaLibraryVersion0() throws IOException {
        LOG.trace("searching for version file: {}", PATH_VERSION_FILE);
        var versionFile = NativeLibrary.class.getResource(PATH_VERSION_FILE); //$NON-NLS-1$
        if (versionFile == null) {
            throw new FileNotFoundException("missing version file of Tsubakuro IPC library");
        }
        LOG.debug("loading version file: {}", versionFile);
        try (var input = versionFile.openStream()) {
            var properties = new Properties();
            properties.load(input);
            return Optional.ofNullable(properties.getProperty(ATTRIBUTE_LIBRARY_VERSION));
        }
    }

    /**
     * Returns the native library version string.
     * @return the native library version string, or empty if it is not declared
     */
    public static Optional<String> getNativeLibraryVersion() {
        var cached = VERSION_NATIVE.get();
        if (cached != null) {
            return cached;
        }
        LOG.trace("retrieving native library version"); //$NON-NLS-1$
        var retrieved = Optional.ofNullable(getNativeLibraryVersionNative())
                .filter(it -> it.length != 0)
                .map(it -> {
                    return new String(it, StandardCharsets.UTF_8);
                });

        LOG.debug("native library version: {}", retrieved); //$NON-NLS-1$
        VERSION_NATIVE.compareAndSet(null, retrieved);
        return retrieved;
    }

    private static native @Nullable byte[] getNativeLibraryVersionNative();

    private NativeLibrary() {
        throw new AssertionError();
    }
}
