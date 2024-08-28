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
package com.tsurugidb.tsubakuro.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class PlanGraphUtilTest {

    private final long seed = 123456789;

    @Test
    void sort_trivial() {
        var n = nodes(1);
        test(seq(n[0]));
    }

    @Test
    void sort_sequence() {
        // 0 - 1 - 2
        var n = nodes(3);
        chain(n[0], n[1], n[2]);
        test(seq(n[0], n[1], n[2]));
    }

    @Test
    void sort_branch() {
        // 0 - 1 -+- 2 - 3
        //        \- 4 - 5

        var n = nodes(6);
        chain(n[0], n[1], n[2], n[3]);
        chain(n[1], n[4], n[5]);
        test(seq(n[0], n[1], or(seq(n[2], n[3]), seq(n[4], n[5]))));
    }

    @Test
    void sort_merge() {
        // 0 - 1 -+- 2 - 3
        // 4 - 5 -/

        var n = nodes(6);
        chain(n[0], n[1], n[2], n[3]);
        chain(n[4], n[5], n[2]);
        test(seq(or(seq(n[0], n[1]), seq(n[4], n[5])), n[2], n[3]));
    }

    @Test
    void sort_orphan() {
        // 0 - 1 - 2
        // 3 - 4 - 5

        var n = nodes(6);
        chain(n[0], n[1], n[2]);
        chain(n[3], n[4], n[5]);
        test(or(seq(n[0], n[1], n[2]), seq(n[3], n[4], n[5])));
    }

    @Test
    void sort_complex() {
        // 0:0 - 1:0 -+- 2:1 ----------+- 3:3
        //            \               /
        // 4:0 ---+----+------+- 5:2-+
        //         \         /        \
        // 6:0 -----+- 7:1 -+----------+- 8:3

        var n = nodes(9);
        chain(n[0], n[1], n[2], n[3]);
        chain(n[1], n[5]);
        chain(n[4], n[5], n[3]);
        chain(n[4], n[7]);
        chain(n[6], n[7], n[5]);
        chain(n[7], n[8]);
        test(seq(
                or(seq(n[0], n[1]), n[4], n[6]),
                or(n[2], n[7]),
                n[5],
                or(n[3], n[8])));
    }

    @Test
    void sort_cyclic_1() {
        var n = nodes(2);
        chain(n[0], n[1], n[0]);
        assertThrows(IllegalArgumentException.class, () -> PlanGraphUtil.sort(Arrays.asList(n)));
    }

    @Test
    void sort_cyclic_2() {
        var n = nodes(3);
        chain(n[0], n[1], n[2], n[1]);
        assertThrows(IllegalArgumentException.class, () -> PlanGraphUtil.sort(Arrays.asList(n))).printStackTrace();
    }

    private void test(Element rule) {
        var nodes = rule.stream().collect(Collectors.toSet());
        var plan = new BasicPlanGraph(nodes);
        assertEquals(plan.getNodes(), nodes);

        var random = new Random(seed);
        var list = new ArrayList<>(nodes);
        for (int i = 0, n = Math.min(100, nodes.size() * 2); i < n; i++) {
            Collections.shuffle(list, random);
            var sorted = PlanGraphUtil.sort(list);
            rule.validate(sorted);
        }
    }

    // graph
    private static BasicPlanNode[] nodes(int count) {
        var results = new BasicPlanNode[count];
        for (int i = 0; i < results.length; i++) {
            results[i] = new BasicPlanNode(String.valueOf(i), Map.of());
        }
        return results;
    }

    private static void chain(BasicPlanNode... series) {
        for (int i = 0; i < series.length - 1; i++) {
            series[i].addDownstream(series[i + 1]);
        }
    }

    // models
    private interface Element {
        Stream<PlanNode> stream();
        void validate(List<PlanNode> nodes);
    }

    private static class Value implements Element {
        private final PlanNode value;
        Value(PlanNode value) {
            this.value = value;
        }
        @Override
        public Stream<PlanNode> stream() {
            return Stream.of(value);
        }
        @Override
        public void validate(List<PlanNode> nodes) {
            return;
        }
    }

    private static class Sequence implements Element {
        private final List<Element> elements;
        Sequence(List<Element> elements) {
            this.elements = new ArrayList<>();
            for (var element : elements) {
                if (element instanceof Sequence) {
                    this.elements.addAll(((Sequence) element).elements);
                } else {
                    this.elements.add(element);
                }
            }
        }
        @Override
        public Stream<PlanNode> stream() {
            return elements.stream().flatMap(Element::stream);
        }
        @Override
        public void validate(List<PlanNode> nodes) {
            elements.forEach(it -> it.validate(nodes));
            for (int i = 0, n = elements.size(); i < n - 1; i++) {
                var lefts = elements.get(i).stream().collect(Collectors.toList());
                var rights = elements.get(i + 1).stream().collect(Collectors.toList());
                for (var left : lefts) {
                    int leftIndex = nodes.indexOf(left);
                    assertNotEquals(-1, leftIndex);
                    for (var right : rights) {
                        int rightIndex = nodes.indexOf(right);
                        assertNotEquals(-1, rightIndex);

                        assertTrue(leftIndex < rightIndex, String.format("%s < %s required", left, right));
                    }
                }
            }
        }
    }

    private static class Selection implements Element {
        private final List<Element> elements;
        Selection(List<Element> elements) {
            this.elements = new ArrayList<>();
            for (var element : elements) {
                if (element instanceof Selection) {
                    this.elements.addAll(((Selection) element).elements);
                } else {
                    this.elements.add(element);
                }
            }
        }
        @Override
        public Stream<PlanNode> stream() {
            return elements.stream().flatMap(Element::stream);
        }
        @Override
        public void validate(List<PlanNode> nodes) {
            elements.forEach(it -> it.validate(nodes));
        }
    }

    // factories

    static Element seq(Object... values) {
        var elements = toElements(values);
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new Sequence(elements);
    }

    static Element or(Object... values) {
        var elements = toElements(values);
        if (elements.size() == 1) {
            return elements.get(0);
        }
        return new Selection(elements);
    }

    private static List<Element> toElements(Object... values) {
        var elements = new ArrayList<Element>();
        for (var value : values) {
            if (value instanceof Element) {
                elements.add((Element) value);
            } else if (value instanceof PlanNode) {
                elements.add(new Value((PlanNode) value));
            } else {
                throw new AssertionError(value);
            }
        }
        return elements;
    }
}
