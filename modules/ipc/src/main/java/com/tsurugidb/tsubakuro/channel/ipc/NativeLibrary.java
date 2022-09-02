package com.tsurugidb.tsubakuro.channel.ipc;

import java.nio.file.Path;
import java.util.Optional;

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
     */
    public static void load() {
        LOG.trace("native library requested");
    }

    private NativeLibrary() {
        throw new AssertionError();
    }
}
