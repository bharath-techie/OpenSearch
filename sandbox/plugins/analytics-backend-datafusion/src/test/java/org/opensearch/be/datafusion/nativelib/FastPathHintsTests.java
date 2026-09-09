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

    public void testTopKSpecMapsBudgetAndFlags() {
        FastPathHints hints = FastPathHints.fromSpec(
            org.opensearch.analytics.spi.FastPathHintSpec.topK(
                42,
                true,  // keepLast
                true,  // pathSafe
                true,  // strippable
                org.opensearch.analytics.spi.FastPathHintSpec.RangeUnit.MILLIS,
                100L,
                200L
            )
        );

        assertEquals(FastPathHints.SHAPE_TOPK, hints.shape());
        assertEquals(42, hints.topkBudget());
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(FastPathHints.BYTE_SIZE);
            hints.writeTo(segment);
            assertEquals((byte) 2, segment.get(ValueLayout.JAVA_BYTE, 1)); // shape = TOPK
            int flags = segment.get(ValueLayout.JAVA_BYTE, 2) & 0xFF;
            assertTrue((flags & FastPathHints.FLAG_TOPK_KEEP_LAST) != 0);
            assertTrue((flags & FastPathHints.FLAG_TOPK_PATH_SAFE) != 0);
            assertEquals(42, segment.get(ValueLayout.JAVA_INT, 4)); // topk_budget
        }
    }

    public void testHistogramSpecMapsBucketOpAndOperand() {
        FastPathHints hints = FastPathHints.fromSpec(
            org.opensearch.analytics.spi.FastPathHintSpec.histogram(
                org.opensearch.analytics.spi.FastPathHintSpec.BucketOp.DIV,
                3600000L,
                true,
                org.opensearch.analytics.spi.FastPathHintSpec.RangeUnit.MILLIS,
                100L,
                200L
            )
        );

        assertEquals(FastPathHints.SHAPE_HISTOGRAM, hints.shape());
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(FastPathHints.BYTE_SIZE);
            hints.writeTo(segment);
            assertEquals((byte) 3, segment.get(ValueLayout.JAVA_BYTE, 1));             // shape = HISTOGRAM
            assertEquals((byte) FastPathHints.BUCKET_OP_DIV, segment.get(ValueLayout.JAVA_BYTE, 24)); // bucket op
            assertEquals(3600000L, segment.get(ValueLayout.JAVA_LONG, 32));            // bucket operand
        }
    }

    public void testHistogramFloorToMultipleMapsToWireOpFive() {
        assertEquals(5, FastPathHints.BUCKET_OP_FLOOR_TO_MULTIPLE);
        FastPathHints hints = FastPathHints.fromSpec(
            org.opensearch.analytics.spi.FastPathHintSpec.histogram(
                org.opensearch.analytics.spi.FastPathHintSpec.BucketOp.FLOOR_TO_MULTIPLE,
                3_600_000L,
                true,
                org.opensearch.analytics.spi.FastPathHintSpec.RangeUnit.MILLIS,
                100L,
                200L
            )
        );

        assertEquals(FastPathHints.SHAPE_HISTOGRAM, hints.shape());
        assertEquals(FastPathHints.BUCKET_OP_FLOOR_TO_MULTIPLE, hints.histogramBucketOp());
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(FastPathHints.BYTE_SIZE);
            hints.writeTo(segment);
            // ordinal of FastPathHintSpec.BucketOp.FLOOR_TO_MULTIPLE must equal the wire discriminant 5.
            assertEquals((byte) 5, segment.get(ValueLayout.JAVA_BYTE, 24));
            assertEquals(3_600_000L, segment.get(ValueLayout.JAVA_LONG, 32));
        }
    }
}
