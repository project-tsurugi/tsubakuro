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
package com.tsurugidb.tsubakuro.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to get the version of Tsubakuro.
 */
public final class TsubakuroVersion {
    static final Logger LOG = LoggerFactory.getLogger(TsubakuroVersion.class);

    private static final String PATH_VERSION_DIR = "/META-INF/tsurugidb/"; //$NON-NLS-1$
    private static final String MODULE_PREFIX = "tsubakuro-"; //$NON-NLS-1$
    private static final String VERSION_FILE_SUFFIX = ".properties"; //$NON-NLS-1$

    private static final String ATTRIBUTE_BUILD_TIMESTAMP = "Build-Timestamp"; //$NON-NLS-1$
    private static final String ATTRIBUTE_BUILD_REVISION = "Build-Revision"; //$NON-NLS-1$
    private static final String ATTRIBUTE_BUILD_VERSION = "Build-Version"; //$NON-NLS-1$

    private static final Map<String, Properties> PROPERTIES_MAP = new ConcurrentHashMap<>();

    private TsubakuroVersion() {
    }

    /**
     * Get Build-Timestamp.
     *
     * @param moduleName module name
     * @return Build-Timestamp
     * @throws IOException if an I/O error occurs loading from the version file
     */
    public static String getBuildTimestamp(String moduleName) throws IOException {
        var properties = getProperties(moduleName);
        return properties.getProperty(ATTRIBUTE_BUILD_TIMESTAMP);
    }

    /**
     * Get Build-Revision.
     *
     * @param moduleName module name
     * @return Build-Revision
     * @throws IOException if an I/O error occurs loading from the version file
     */
    public static String getBuildRevision(String moduleName) throws IOException {
        var properties = getProperties(moduleName);
        return properties.getProperty(ATTRIBUTE_BUILD_REVISION);
    }

    /**
     * Get Build-Version.
     *
     * @param moduleName module name
     * @return Build-Version
     * @throws IOException if an I/O error occurs loading from the version file
     */
    public static String getBuildVersion(String moduleName) throws IOException {
        var properties = getProperties(moduleName);
        return properties.getProperty(ATTRIBUTE_BUILD_VERSION);
    }

    /**
     * Get version properties.
     *
     * @param moduleName module name
     * @return version properties
     * @throws IOException if an I/O error occurs loading from the version file
     */
    public static Properties getProperties(String moduleName) throws IOException {
        try {
            return PROPERTIES_MAP.computeIfAbsent(moduleName, key -> {
                String module = MODULE_PREFIX + key;
                try {
                    return loadProperties(module);
                } catch (Exception e) {
                    LOG.debug("version file load error. module={}", module, e); // $NON-NLS-1$
                    throw new UncheckedIOException(new IOException(MessageFormat.format("version file load error. module={0}", module), e));
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static Properties loadProperties(String module) throws IOException {
        String path = PATH_VERSION_DIR + module + VERSION_FILE_SUFFIX;
        LOG.trace("searching for version file: {}", path); // $NON-NLS-1$
        var versionFile = TsubakuroVersion.class.getResource(path);
        if (versionFile == null) {
            throw new FileNotFoundException(MessageFormat.format("missing version file. path={0}", path));
        }
        LOG.debug("loading version file: {}", versionFile); // $NON-NLS-1$
        try (var input = versionFile.openStream()) {
            var properties = new Properties();
            properties.load(input);
            return properties;
        }
    }
}
