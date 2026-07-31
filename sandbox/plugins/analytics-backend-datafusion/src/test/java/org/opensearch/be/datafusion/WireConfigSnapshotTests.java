/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class WireConfigSnapshotTests extends OpenSearchTestCase {

    public void testByteSize() {
        assertEquals(64L, WireConfigSnapshot.BYTE_SIZE);
    }

    public void testWriteToWritesCorrectValuesAtCorrectOffsets() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder()
            .batchSize(8192)
            .targetPartitions(4)
            .listingTablePushdownFilters(true)
            .build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            // abi_version guard at offset 0; Rust asserts it matches.
            assertEquals(WireConfigSnapshot.ABI_VERSION, segment.get(ValueLayout.JAVA_INT, 0));
            assertEquals(8192L, segment.get(ValueLayout.JAVA_LONG, 8));
            assertEquals(4L, segment.get(ValueLayout.JAVA_LONG, 16));
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 24)); // listing_table_pushdown = true
        }
    }

    public void testWriteToWritesListingTablePushdownFalseAsZero() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().listingTablePushdownFilters(false).build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(0, segment.get(ValueLayout.JAVA_INT, 24));
        }
    }

    public void testHardcodedFieldsAreWrittenCorrectly() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().batchSize(16384).targetPartitions(8).build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 28));  // indexed_pushdown_filters default (true)
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 32));  // cost_predicate (hardcoded)
            assertEquals(10, segment.get(ValueLayout.JAVA_INT, 36)); // cost_collector (hardcoded)
        }
    }

    public void testAbiVersionIsWrittenAtOffsetZero() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().build();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            assertEquals(WireConfigSnapshot.ABI_VERSION, segment.get(ValueLayout.JAVA_INT, 0));
            // Padding at offset 4 is deterministically zero.
            assertEquals(0, segment.get(ValueLayout.JAVA_INT, 4));
        }
    }

    public void testIndexedPushdownFiltersIsWrittenFromSnapshot() {
        for (boolean v : new boolean[] { true, false }) {
            WireConfigSnapshot snapshot = WireConfigSnapshot.builder().indexedPushdownFilters(v).build();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                snapshot.writeTo(segment);
                assertEquals(v ? 1 : 0, segment.get(ValueLayout.JAVA_INT, 28));
            }
            assertEquals(v, snapshot.indexedPushdownFilters());
        }
    }

    /**
     * The last field ends at {@link WireConfigSnapshot#BYTE_SIZE}: nothing is written
     * past {@code indexed_decode_time_refinement} at offset 60. A field added on the
     * Rust side without growing {@code BYTE_SIZE} here would read uninitialized memory.
     */
    public void testNothingIsWrittenPastTheLastField() {
        assertEquals(64L, WireConfigSnapshot.BYTE_SIZE);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE + 8);
            segment.fill((byte) 0x7f);
            WireConfigSnapshot.builder().build().writeTo(segment);
            assertEquals(0x7f7f7f7fL, segment.get(ValueLayout.JAVA_INT, 64) & 0xffffffffL);
        }
    }

    /** The four granularity/refinement fields land at 40/48/56/60. */
    public void testGranularityFieldsAreWrittenAtCorrectOffsets() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder()
            .minSkipRunDefault(512)
            .minSkipRunSelectivityThreshold(0.07)
            .forceStrategy(WireConfigSnapshot.FORCE_STRATEGY_BOOLEAN_MASK)
            .indexedDecodeTimeRefinement(true)
            .build();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            assertEquals(512L, segment.get(ValueLayout.JAVA_LONG, 40));
            assertEquals(0.07, segment.get(ValueLayout.JAVA_DOUBLE, 48), 0.0);
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 56));
            assertEquals(1, segment.get(ValueLayout.JAVA_INT, 60));
        }
    }

    /**
     * {@code force_strategy} carries {@code -1} for "let selectivity decide", so it
     * must be written as a signed int rather than clamped to zero.
     */
    public void testForceStrategyNoneIsWrittenAsMinusOne() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().build();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            assertEquals(-1, segment.get(ValueLayout.JAVA_INT, 56));
        }
    }

    public void testBuilderDefaultsMatchExpected() {
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder().build();

        assertEquals(8192, snapshot.batchSize());
        assertEquals(4, snapshot.targetPartitions());
        assertEquals(false, snapshot.listingTablePushdownFilters());
        assertEquals(true, snapshot.indexedPushdownFilters());
        assertEquals(1024, snapshot.minSkipRunDefault());
        assertEquals(0.03, snapshot.minSkipRunSelectivityThreshold(), 0.0);
        assertEquals(WireConfigSnapshot.FORCE_STRATEGY_NONE, snapshot.forceStrategy());
        // Decode-time refinement is a second decode pass; off unless measured to pay off.
        assertEquals(false, snapshot.indexedDecodeTimeRefinement());
    }

    public void testBuilderCopyPreservesAllFields() {
        WireConfigSnapshot original = WireConfigSnapshot.builder()
            .batchSize(4096)
            .targetPartitions(16)
            .listingTablePushdownFilters(true)
            .indexedPushdownFilters(false)
            .minSkipRunDefault(256)
            .minSkipRunSelectivityThreshold(0.11)
            .forceStrategy(WireConfigSnapshot.FORCE_STRATEGY_ROW_SELECTION)
            .indexedDecodeTimeRefinement(true)
            .build();

        WireConfigSnapshot copy = WireConfigSnapshot.builder(original).build();

        assertEquals(original.batchSize(), copy.batchSize());
        assertEquals(original.targetPartitions(), copy.targetPartitions());
        assertEquals(original.listingTablePushdownFilters(), copy.listingTablePushdownFilters());
        assertEquals(original.indexedPushdownFilters(), copy.indexedPushdownFilters());
        assertEquals(original.minSkipRunDefault(), copy.minSkipRunDefault());
        assertEquals(original.minSkipRunSelectivityThreshold(), copy.minSkipRunSelectivityThreshold(), 0.0);
        assertEquals(original.forceStrategy(), copy.forceStrategy());
        assertEquals(original.indexedDecodeTimeRefinement(), copy.indexedDecodeTimeRefinement());
    }
}
