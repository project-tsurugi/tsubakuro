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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * Extracts properties from {@code join_scan}.
 */
public class JoinScanPropertyExtractor implements PropertyExtractor {

    @Override
    public Optional<String> getTitle(ObjectNode object) throws PlanGraphException {
        return Optional.of("join");
    }

    @Override
    public Map<String, String> getAttributes(@Nonnull ObjectNode object) throws PlanGraphException {
        Objects.requireNonNull(object);
        JsonUtil.checkKind(object, "join_scan");
        var info = PropertyExtractorUtil.findSourceIndex(object);
        var access = PropertyExtractorUtil.hasScanRange(object) ? "range-scan" : "full-scan";
        var kind = JsonUtil.getString(object, "operator_kind");
        if (info.isEmpty()) {
            return PropertyExtractorUtil.attributes(
                    "join-type", kind,
                    "source", "broadcast",
                    "access", access);
        }
        var results = new LinkedHashMap<String, String>();
        results.put("join-type", kind);
        results.putAll(info.get().toAttributes());
        results.put("access", access);
        return results;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
