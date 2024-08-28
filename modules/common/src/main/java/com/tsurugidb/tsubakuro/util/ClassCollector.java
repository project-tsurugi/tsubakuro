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

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects declared classes from its definition files in class-path.
 */
public final class ClassCollector {

    static final Logger LOG = LoggerFactory.getLogger(ClassCollector.class);

    /**
     * Character set encoding of definition files.
     */
    public static final Charset ENCODING_METADATA = StandardCharsets.UTF_8;

    private static final char COMMENT_START = '#';

    /**
     * Returns a list of available subclasses of the specified class, listed in the definition file.
     * @param <T>  the service class type
     * @param serviceClass the service class
     * @param loader the class loader to load the subclasses
     * @param definitionFilePath path to the definition file in the classpath
     * @param failOnError {@code true} to raise and error if any exceptions are occurred,
     *      or {@code false} to ignore such the entries and only record logs
     * @return available subclasses
     * @throws IOException if failed to load the definition files
     * @throws IOException if the definitions are not valid when {@code failOnError} is {@code true}
     */
    public static <T> List<Class<? extends T>> collect(
            @Nonnull Class<T> serviceClass,
            @Nonnull ClassLoader loader,
            @Nonnull String definitionFilePath,
            boolean failOnError) throws IOException {
        Objects.requireNonNull(loader);
        var results = new ArrayList<Class<? extends T>>();
        LOG.debug("loading {}", definitionFilePath); //$NON-NLS-1$
        var resources = loader.getResources(definitionFilePath);
        while (resources.hasMoreElements()) {
            var element = resources.nextElement();
            LOG.debug("found definition file of {}: {}", serviceClass.getName(), element); //$NON-NLS-1$

            var found = collectFromUrl(element, serviceClass, loader, failOnError);
            results.addAll(found);
        }
        return results;
    }

    private static <T> List<Class<? extends T>> collectFromUrl(
            URL url,
            Class<T> serviceClass, ClassLoader loader,
            boolean failOnError) throws IOException {
        var results = new ArrayList<Class<? extends T>>();
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

                LOG.debug("loading class: {}", className); //$NON-NLS-1$
                try {
                    var clazz = loader.loadClass(className);
                    results.add(clazz.asSubclass(serviceClass));
                } catch (ClassNotFoundException e) {
                    var message = MessageFormat.format(
                            "cannot load a class: {0} ({1})",
                            className,
                            url);
                    if (failOnError) {
                        throw new IOException(message, e);
                    }
                    LOG.warn(message);
                } catch (ClassCastException e) {
                    var message = MessageFormat.format(
                            "not a subclass of {0}: {1} ({2})",
                            serviceClass.getName(),
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
                    "error occurred while loading definition file: {0}",
                    url));
        }
        return results;
    }


    private ClassCollector() {
        throw new AssertionError();
    }
}
