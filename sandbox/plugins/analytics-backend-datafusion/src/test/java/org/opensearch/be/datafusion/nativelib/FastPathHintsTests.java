/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class FastPathHintsTests extends OpenSearchTestCase {

    public void testByteSize() {
        // Must match the Rust FASTPATHHINTS_BYTE_SIZE (fast_path_hints.rs).
        assertEquals(40L, FastPathHints.BYTE_SIZE);
    }

    public void testWriteToWritesCorrectValuesAtCorrectOffsets() {
        FastPathHints hints = new FastPathHints(
            FastPathHints.VERSION,
            FastPathHints.SHAPE_COUNT_ONLY,
            FastPathHints.FLAG_RANGE_ON_LEADING_SORT_FIELD | FastPathHints.FLAG_RANGE_CONJUNCT_STRIPPABLE,
            FastPathHints.UNIT_MILLIS,
            0,
            100L,
            200L,
            0,
            0L
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(FastPathHints.BYTE_SIZE);
            hints.writeTo(segment);

            assertEquals((byte) 1, segment.get(ValueLayout.JAVA_BYTE, 0));  // version
            assertEquals((byte) 1, segment.get(ValueLayout.JAVA_BYTE, 1));  // shape = COUNT_ONLY
            assertEquals((byte) 0b11, segment.get(ValueLayout.JAVA_BYTE, 2)); // both range flags
            assertEquals((byte) 2, segment.get(ValueLayout.JAVA_BYTE, 3));  // range_unit = millis
            assertEquals(0, segment.get(ValueLayout.JAVA_INT, 4));          // topk_budget reserved
            assertEquals(100L, segment.get(ValueLayout.JAVA_LONG, 8));      // lower
            assertEquals(200L, segment.get(ValueLayout.JAVA_LONG, 16));     // upper
            assertEquals((byte) 0, segment.get(ValueLayout.JAVA_BYTE, 24)); // histogram_op reserved
            assertEquals(0L, segment.get(ValueLayout.JAVA_LONG, 32));       // histogram_operand reserved
        }
    }

    public void testNoneIsAllZeroExceptVersion() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(FastPathHints.BYTE_SIZE);
            FastPathHints.NONE.writeTo(segment);

            assertEquals((byte) 1, segment.get(ValueLayout.JAVA_BYTE, 0)); // version
            assertEquals((byte) 0, segment.get(ValueLayout.JAVA_BYTE, 1)); // shape NONE
            assertEquals((byte) 0, segment.get(ValueLayout.JAVA_BYTE, 2)); // no flags
            assertEquals((byte) 0, segment.get(ValueLayout.JAVA_BYTE, 3)); // unit none
            assertEquals(0L, segment.get(ValueLayout.JAVA_LONG, 8));
            assertEquals(0L, segment.get(ValueLayout.JAVA_LONG, 16));
        }
    }

    public void testAbsentBoundSentinelsRoundTrip() {
        FastPathHints hints = new FastPathHints(
            FastPathHints.VERSION,
            FastPathHints.SHAPE_COUNT_ONLY,
            FastPathHints.FLAG_RANGE_ON_LEADING_SORT_FIELD,
            FastPathHints.UNIT_NANOS,
            0,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0,
            0L
        );

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(FastPathHints.BYTE_SIZE);
            hints.writeTo(segment);
            assertEquals(Long.MIN_VALUE, segment.get(ValueLayout.JAVA_LONG, 8));
            assertEquals(Long.MAX_VALUE, segment.get(ValueLayout.JAVA_LONG, 16));
        }
    }
}
