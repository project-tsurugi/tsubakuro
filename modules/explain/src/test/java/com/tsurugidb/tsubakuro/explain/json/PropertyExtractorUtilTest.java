/*
 * Copyright 2023-2026 Project Tsurugi.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

class PropertyExtractorUtilTest {

    private static JsonNode json(String text) throws IOException {
        return new ObjectMapper()
                .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                .readTree(text);
    }

    private static Optional<Map<String, String>> rowSlice(JsonNode node) throws PlanGraphException {
        return PropertyExtractorUtil.findRowSlice(node).map(PropertyExtractorUtil.RowSliceInfo::toAttributes);
    }

    @Test
    void rowSliceCount() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {'count': {'kind': 'immediate', 'element': 5}}}"));
        assertEquals(Optional.of(Map.of("limit", "5")), attrs);
    }

    @Test
    void rowSliceStart() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {'start': {'kind': 'immediate', 'element': 10}}}"));
        assertEquals(Optional.of(Map.of("offset", "10")), attrs);
    }

    @Test
    void rowSliceStartCount() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {'start': {'kind': 'immediate', 'element': 10}, 'count': {'kind': 'immediate', 'element': 5}}}"));
        assertEquals(Optional.of(Map.of("offset", "10", "limit", "5")), attrs);
        assertEquals(List.of("offset", "limit"), List.copyOf(attrs.get().keySet()));
    }

    @Test
    void rowSliceStartZero() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {'start': {'kind': 'immediate', 'element': 0}, 'count': {'kind': 'immediate', 'element': 5}}}"));
        assertEquals(Optional.of(Map.of("limit", "5")), attrs);
    }

    @Test
    void rowSliceVariable() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {'start': {'kind': 'variable', 'element': {'kind': 'variable', 'entity': '3'}}, 'count': {'kind': 'variable', 'element': {'kind': 'variable', 'entity': '4'}}}}"));
        assertEquals(Optional.of(Map.of("offset", "?", "limit", "?")), attrs);
    }

    @Test
    void rowSliceEmpty() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {}}"));
        assertEquals(Optional.empty(), attrs);
    }

    @Test
    void rowSliceLegacy() throws Exception {
        var attrs = rowSlice(json("{'limit': 1}"));
        assertEquals(Optional.of(Map.of("limit", "1")), attrs);
    }

    @Test
    void rowSliceAbsent() throws Exception {
        var attrs = rowSlice(json("{}"));
        assertEquals(Optional.empty(), attrs);
    }

    @Test
    void rowSliceIgnoreLegacy() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {}, 'limit': 1}"));
        assertEquals(Optional.empty(), attrs);
    }

    @Test
    void rowSliceUnknownKind() throws Exception {
        var attrs = rowSlice(json("{'row_slice': {'count': {'kind': 'unknown', 'element': 1}}}"));
        assertEquals(Optional.empty(), attrs);
    }

    @Test
    void rowSliceNotObject() throws Exception {
        var node = json("{'row_slice': 'x'}");
        assertThrows(PlanGraphException.class, () -> rowSlice(node));
    }

    @Test
    void rowSliceValueNotObject() throws Exception {
        var node = json("{'row_slice': {'count': 1}}");
        assertThrows(PlanGraphException.class, () -> rowSlice(node));
    }

    @Test
    void rowSliceValueMissingKind() throws Exception {
        var node = json("{'row_slice': {'count': {'element': 1}}}");
        assertThrows(PlanGraphException.class, () -> rowSlice(node));
    }

    @Test
    void rowSliceImmediateMissingElement() throws Exception {
        var node = json("{'row_slice': {'count': {'kind': 'immediate'}}}");
        assertThrows(PlanGraphException.class, () -> rowSlice(node));
    }

    @Test
    void rowSliceImmediateNotInteger() throws Exception {
        var node = json("{'row_slice': {'count': {'kind': 'immediate', 'element': 'x'}}}");
        assertThrows(PlanGraphException.class, () -> rowSlice(node));
    }

    @Test
    void scanDirectionForward() throws Exception {
        var direction = PropertyExtractorUtil.findScanDirection(json("{'direction': 'forward'}"));
        assertEquals(Optional.of("forward"), direction);
    }

    @Test
    void scanDirectionBackward() throws Exception {
        var direction = PropertyExtractorUtil.findScanDirection(json("{'direction': 'backward'}"));
        assertEquals(Optional.of("backward"), direction);
    }

    @Test
    void scanDirectionAbsent() throws Exception {
        var direction = PropertyExtractorUtil.findScanDirection(json("{}"));
        assertTrue(direction.isEmpty());
    }

    @Test
    void scanDirectionUnknown() throws Exception {
        var direction = PropertyExtractorUtil.findScanDirection(json("{'direction': 'sideways'}"));
        assertTrue(direction.isEmpty());
    }

    @Test
    void scanDirectionNotText() throws Exception {
        var node = json("{'direction': 1}");
        assertThrows(PlanGraphException.class, () -> PropertyExtractorUtil.findScanDirection(node));
    }

    @Test
    void forwardOrderingAbsent() throws Exception {
        var ordering = PropertyExtractorUtil.findForwardOrdering(json("{}"));
        assertEquals(Set.of(), ordering);
    }

    @Test
    void forwardOrderingNull() throws Exception {
        var ordering = PropertyExtractorUtil.findForwardOrdering(json("{'ordering': null}"));
        assertEquals(Set.of(), ordering);
    }

    @Test
    void forwardOrderingDuplicate() throws Exception {
        var ordering = PropertyExtractorUtil.findForwardOrdering(json("{'ordering': ['respect_input', 'respect_input']}"));
        assertEquals(Set.of("respect_input"), ordering);
    }

    @Test
    void forwardOrderingNotText() throws Exception {
        var node = json("{'ordering': [1]}");
        assertThrows(PlanGraphException.class, () -> PropertyExtractorUtil.findForwardOrdering(node));
    }
}
