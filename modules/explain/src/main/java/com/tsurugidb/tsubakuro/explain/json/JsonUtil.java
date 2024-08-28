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
package com.tsurugidb.tsubakuro.explain.json;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

final class JsonUtil {

    private static final String K_KIND = "kind"; //$NON-NLS-1$

    private static final String K_THIS = "this"; //$NON-NLS-1$

    private static final String MESSAGE_NOT_AVAILABLE = "N/A"; //$NON-NLS-1$

    static String getKind(JsonNode node) throws PlanGraphException {
        var elem = get(node, K_KIND);
        if (!elem.isTextual()) {
            throw new PlanGraphException(MessageFormat.format(
                    "\"kind\" field must be a text: {0}",
                    elem));
        }
        return elem.asText();
    }

    static String getThis(JsonNode node) throws PlanGraphException {
        var elem = get(node, K_THIS);
        return elem.asText();
    }

    static void checkKind(JsonNode node, String name) throws PlanGraphException {
        var actual = getKind(node);
        if (!Objects.equals(name, actual)) {
            throw new PlanGraphException(MessageFormat.format(
                    "broken explain information: invalid object kind: \"{0}\" (expected: \"{1}\")",
                    actual,
                    name));
        }
    }

    static void checkKind(JsonNode node, Set<String> candidates) throws PlanGraphException {
        var actual = getKind(node);
        if (!candidates.contains(actual)) {
            throw new PlanGraphException(MessageFormat.format(
                    "broken explain information: invalid object kind: \"{0}\" (expected: \"{1}\")",
                    actual,
                    candidates));
        }
    }

    static boolean isKind(JsonNode node, String name) throws PlanGraphException {
        return Objects.equals(getKind(node), name);
    }

    static @Nonnull JsonNode get(JsonNode node, String name) throws PlanGraphException {
        var elem = node.get(name);
        if (elem == null || elem.isMissingNode() || elem.isNull()) {
            if (Objects.equals(name, K_KIND)) {
                throw new PlanGraphException(MessageFormat.format("missing property \"{0}\"",  name));
            }
            throw new PlanGraphException(MessageFormat.format(
                    "missing property \"{0}\" in kind of \"{1}\"",
                    name,
                    findKind(node).orElse(MESSAGE_NOT_AVAILABLE)));

        }
        return elem;
    }

    static Optional<String> findKind(JsonNode node) {
        return Optional.ofNullable(node.get(K_KIND))
                .filter(it -> !it.isNull())
                .filter(it -> !it.isMissingNode())
                .map(JsonNode::asText);
    }

    static @Nonnull ObjectNode getObject(JsonNode node, String name) throws PlanGraphException {
        var elem = get(node, name);
        if (!elem.isObject()) {
            throw new PlanGraphException(MessageFormat.format(
                    "invalid property \"{0}\" in kind of \"{1}\", must be an object",
                    name,
                    findKind(elem).orElse(MESSAGE_NOT_AVAILABLE)));
        }
        return (ObjectNode) elem;
    }

    static @Nonnull ArrayNode getArray(JsonNode node, String name) throws PlanGraphException {
        var elem = get(node, name);
        if (!elem.isArray()) {
            throw new PlanGraphException(MessageFormat.format(
                    "invalid property \"{0}\" in kind of \"{1}\", must be an array",
                    name,
                    findKind(elem).orElse(MESSAGE_NOT_AVAILABLE)));
        }
        return (ArrayNode) elem;
    }

    static @Nonnull String getString(JsonNode node, String name) throws PlanGraphException {
        var elem = get(node, name);
        if (!elem.isTextual()) {
            throw new PlanGraphException(MessageFormat.format(
                    "invalid property \"{0}\" in kind of \"{1}\", must be a text",
                    name,
                    findKind(elem).orElse(MESSAGE_NOT_AVAILABLE)));
        }
        return elem.asText();
    }

    static @Nonnull List<String> getStringArray(JsonNode node, String name) throws PlanGraphException {
        var elem = get(node, name);
        var elements = getArray(node, name);
        var results = new ArrayList<String>(elements.size());
        for (int i = 0, n = elements.size(); i < n; i++) {
            var element = elements.get(i);
            if (!element.isTextual()) {
                throw new PlanGraphException(MessageFormat.format(
                        "invalid property \"{0}[{1}]\" in kind of \"{2}\", must be string",
                        name,
                        i,
                        findKind(elem).orElse(MESSAGE_NOT_AVAILABLE)));
            }
            results.add(element.asText());
        }
        return results;
    }

    static OptionalLong findInteger(JsonNode node, String name) throws PlanGraphException {
        var elem = node.get(name);
        if (elem == null || elem.isMissingNode() || elem.isNull()) {
            return OptionalLong.empty();
        }
        if (!elem.isValueNode()) {
            throw new PlanGraphException(MessageFormat.format(
                    "invalid property \"{0}\" in kind of \"{1}\", must be an integer",
                    name,
                    findKind(elem).orElse(MESSAGE_NOT_AVAILABLE)));
        }
        try {
            return OptionalLong.of(Long.parseLong(elem.asText()));
        } catch (NumberFormatException e) {
            throw new PlanGraphException(MessageFormat.format(
                    "invalid property \"{0}\" in kind of \"{1}\", must be an integer",
                    name,
                    findKind(elem).orElse(MESSAGE_NOT_AVAILABLE)), e);
        }
    }

    private JsonUtil() {
        throw new AssertionError();
    }
}
