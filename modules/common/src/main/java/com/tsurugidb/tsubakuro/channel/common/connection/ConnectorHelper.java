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

import java.net.URI;
import java.text.MessageFormat;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectorHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorHelper.class);

    static Connector create(URI endpoint) {
        Objects.requireNonNull(endpoint);
        LOG.trace("creating connector: {}", endpoint); //$NON-NLS-1$
        for (var factory : ServiceLoader.load(ConnectorFactory.class, ConnectorHelper.class.getClassLoader())) {
            var connectorOpt = factory.tryCreate(endpoint);
            if (connectorOpt.isPresent()) {
                LOG.trace("found ConnectorFactory: {} - {}", factory, endpoint); //$NON-NLS-1$
                return connectorOpt.get();
            }
        }
        throw new NoSuchElementException(MessageFormat.format(
                "suitable connector is not found: {0}",
                endpoint));
    }

    private ConnectorHelper() {
        throw new AssertionError();
    }
}
