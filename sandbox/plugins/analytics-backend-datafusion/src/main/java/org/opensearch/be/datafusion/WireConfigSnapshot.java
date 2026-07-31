/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Immutable snapshot of the dynamic indexed query settings, ready to be written
 * into a {@code MemorySegment} matching the Rust {@code WireDatafusionQueryConfig}
 * {@code #[repr(C)]} layout.
 * <p>
 * Use {@link #builder()} to construct instances.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class WireConfigSnapshot {

    /** Total byte size of the wire struct ({@code WireDatafusionQueryConfig}). */
    public static final long BYTE_SIZE = 64;

    /**
     * FFM layout version written at offset 0 and verified by Rust.
     * <p>
     * The struct crosses the boundary as raw bytes with no negotiation, so a
     * Java plugin and a Rust {@code .so} from different builds would silently
     * misread every field rather than fail. Bump this on both sides whenever a
     * field is added, removed, reordered, or resized — it must stay in lockstep
     * with {@code WIRE_CONFIG_ABI_VERSION} in {@code datafusion_query_config.rs}.
     * <p>
     * Layouts before this field existed were unversioned, so there is no
     * version 0 to stay compatible with — a mismatched pair simply fails the
     * assertion on the Rust side.
     */
    public static final int ABI_VERSION = 3;

    /** {@link #forceStrategy()} sentinel: let candidate selectivity decide. */
    public static final int FORCE_STRATEGY_NONE = -1;
    /** {@link #forceStrategy()} sentinel: always row-granular. */
    public static final int FORCE_STRATEGY_ROW_SELECTION = 0;
    /** {@link #forceStrategy()} sentinel: always one whole-row-group select. */
    public static final int FORCE_STRATEGY_BOOLEAN_MASK = 1;

    private final int batchSize;
    private final int targetPartitions;
    private final boolean listingTablePushdownFilters;
    private final boolean indexedPushdownFilters;
    private final int minSkipRunDefault;
    private final double minSkipRunSelectivityThreshold;
    private final int forceStrategy;
    private final boolean indexedDecodeTimeRefinement;

    private WireConfigSnapshot(Builder builder) {
        this.batchSize = builder.batchSize;
        this.targetPartitions = builder.targetPartitions;
        this.listingTablePushdownFilters = builder.listingTablePushdownFilters;
        this.indexedPushdownFilters = builder.indexedPushdownFilters;
        this.minSkipRunDefault = builder.minSkipRunDefault;
        this.minSkipRunSelectivityThreshold = builder.minSkipRunSelectivityThreshold;
        this.forceStrategy = builder.forceStrategy;
        this.indexedDecodeTimeRefinement = builder.indexedDecodeTimeRefinement;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder pre-populated with all values from an existing snapshot.
     * Useful for rebuilding a snapshot with a single field changed.
     */
    public static Builder builder(WireConfigSnapshot current) {
        return new Builder().batchSize(current.batchSize)
            .targetPartitions(current.targetPartitions)
            .listingTablePushdownFilters(current.listingTablePushdownFilters)
            .indexedPushdownFilters(current.indexedPushdownFilters)
            .minSkipRunDefault(current.minSkipRunDefault)
            .minSkipRunSelectivityThreshold(current.minSkipRunSelectivityThreshold)
            .forceStrategy(current.forceStrategy)
            .indexedDecodeTimeRefinement(current.indexedDecodeTimeRefinement);
    }

    public int batchSize() {
        return batchSize;
    }

    public int targetPartitions() {
        return targetPartitions;
    }

    public boolean listingTablePushdownFilters() {
        return listingTablePushdownFilters;
    }

    public boolean indexedPushdownFilters() {
        return indexedPushdownFilters;
    }

    /**
     * Skip runs shorter than this are absorbed into the surrounding {@code select},
     * trading a few over-read rows for a shorter selector list. {@code 1} disables
     * coalescing. Applied only at or above {@link #minSkipRunSelectivityThreshold()}.
     */
    public int minSkipRunDefault() {
        return minSkipRunDefault;
    }

    /**
     * Candidate selectivity (matched rows / row-group rows) below which the
     * selection stays row-granular: sparse candidates make long skips, so each
     * one saves real bytes and coalescing would over-read for nothing.
     */
    public double minSkipRunSelectivityThreshold() {
        return minSkipRunSelectivityThreshold;
    }

    /**
     * Pins the granularity decision instead of letting selectivity choose.
     * Diagnostics only. One of {@link #FORCE_STRATEGY_NONE},
     * {@link #FORCE_STRATEGY_ROW_SELECTION}, {@link #FORCE_STRATEGY_BOOLEAN_MASK}.
     */
    public int forceStrategy() {
        return forceStrategy;
    }

    /**
     * Whether refinement runs as a parquet {@code ArrowPredicate} during decode
     * rather than on the fully decoded batch. Decode-time refinement is two decode
     * passes — the refinement's own columns for every candidate, then the projection
     * for the survivors — so it pays off only when refinement rejects most
     * candidates.
     */
    public boolean indexedDecodeTimeRefinement() {
        return indexedDecodeTimeRefinement;
    }

    /**
     * Writes this snapshot into a {@code MemorySegment} matching the
     * {@code WireDatafusionQueryConfig} {@code #[repr(C)]} layout.
     * <p>
     * The segment must be at least {@link #BYTE_SIZE} bytes and allocated from
     * a confined {@code Arena} scoped to the query lifetime. Fields that the Rust
     * side no longer exposes as settings ({@code cost_predicate},
     * {@code cost_collector}) are written as their fixed hardcoded values.
     *
     * <pre>
     * Offset  Size  Field                                Type     Source
     * ──────  ────  ─────────────────────────────────    ──────   ───────────
     * 0       4     abi_version                          i32      {@link #ABI_VERSION}
     * 4       4     (padding)                            i32      zero
     * 8       8     batch_size                           i64      from snapshot
     * 16      8     target_partitions                    i64      from snapshot
     * 24      4     listing_table_pushdown_filters       i32      from snapshot (0/1)
     * 28      4     indexed_pushdown_filters             i32      from snapshot (0/1)
     * 32      4     cost_predicate                       i32      hardcoded 1
     * 36      4     cost_collector                       i32      hardcoded 10
     * 40      8     min_skip_run_default                 i64      from snapshot
     * 48      8     min_skip_run_selectivity_threshold   f64      from snapshot
     * 56      4     force_strategy                       i32      from snapshot (-1/0/1)
     * 60      4     indexed_decode_time_refinement       i32      from snapshot (0/1)
     * ──────  ────
     * Total: 64 bytes, 8-byte aligned (repr(C), no tail padding needed)
     * </pre>
     *
     * <p>{@code indexed_multi_rg_decode} and {@code route_pure_parquet_through_indexed}
     * were removed with the legacy per-row-group driver: the indexed scan now always
     * runs a single DataFusion scan per chunk, and routing is decided by delegation
     * and the {@code __row_id__} projection alone.
     *
     * @param segment the target memory segment (at least {@link #BYTE_SIZE} bytes)
     */
    public void writeTo(MemorySegment segment) {
        // Offset 0: abi_version (i32) — Rust asserts this matches its own constant
        segment.set(ValueLayout.JAVA_INT, 0, ABI_VERSION);
        // Offset 4: explicit padding, zeroed so the bytes are deterministic
        segment.set(ValueLayout.JAVA_INT, 4, 0);
        // Offset 8: batch_size (i64)
        segment.set(ValueLayout.JAVA_LONG, 8, (long) batchSize);
        // Offset 16: target_partitions (i64)
        segment.set(ValueLayout.JAVA_LONG, 16, (long) targetPartitions);
        // Offset 24: listing_table_pushdown_filters (i32) — 0 = false, 1 = true
        segment.set(ValueLayout.JAVA_INT, 24, listingTablePushdownFilters ? 1 : 0);
        // Offset 28: indexed_pushdown_filters (i32) — 0 = false, 1 = true
        segment.set(ValueLayout.JAVA_INT, 28, indexedPushdownFilters ? 1 : 0);
        // Offset 32: cost_predicate (i32) — hardcoded 1
        segment.set(ValueLayout.JAVA_INT, 32, 1);
        // Offset 36: cost_collector (i32) — hardcoded 10
        segment.set(ValueLayout.JAVA_INT, 36, 10);
        // Offset 40: min_skip_run_default (i64)
        segment.set(ValueLayout.JAVA_LONG, 40, (long) minSkipRunDefault);
        // Offset 48: min_skip_run_selectivity_threshold (f64)
        segment.set(ValueLayout.JAVA_DOUBLE, 48, minSkipRunSelectivityThreshold);
        // Offset 56: force_strategy (i32) — -1 = None, 0 = RowSelection, 1 = BooleanMask
        segment.set(ValueLayout.JAVA_INT, 56, forceStrategy);
        // Offset 60: indexed_decode_time_refinement (i32) — 0 = false, 1 = true
        segment.set(ValueLayout.JAVA_INT, 60, indexedDecodeTimeRefinement ? 1 : 0);
    }

    /**
     * Builder for {@link WireConfigSnapshot}. All fields have sensible defaults
     * matching the Rust {@code DatafusionQueryConfig::fallback()}.
     */
    public static final class Builder {
        private int batchSize = 8192;
        private int targetPartitions = 4;
        private boolean listingTablePushdownFilters = false;
        private boolean indexedPushdownFilters = true;
        private int minSkipRunDefault = 1024;
        private double minSkipRunSelectivityThreshold = 0.03;
        private int forceStrategy = FORCE_STRATEGY_NONE;
        private boolean indexedDecodeTimeRefinement = false;

        private Builder() {}

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder targetPartitions(int targetPartitions) {
            this.targetPartitions = targetPartitions;
            return this;
        }

        public Builder listingTablePushdownFilters(boolean listingTablePushdownFilters) {
            this.listingTablePushdownFilters = listingTablePushdownFilters;
            return this;
        }

        public Builder indexedPushdownFilters(boolean indexedPushdownFilters) {
            this.indexedPushdownFilters = indexedPushdownFilters;
            return this;
        }

        public Builder minSkipRunDefault(int minSkipRunDefault) {
            this.minSkipRunDefault = minSkipRunDefault;
            return this;
        }

        public Builder minSkipRunSelectivityThreshold(double minSkipRunSelectivityThreshold) {
            this.minSkipRunSelectivityThreshold = minSkipRunSelectivityThreshold;
            return this;
        }

        /** @param forceStrategy -1 = None (heuristic), 0 = RowSelection, 1 = BooleanMask. */
        public Builder forceStrategy(int forceStrategy) {
            this.forceStrategy = forceStrategy;
            return this;
        }

        public Builder indexedDecodeTimeRefinement(boolean indexedDecodeTimeRefinement) {
            this.indexedDecodeTimeRefinement = indexedDecodeTimeRefinement;
            return this;
        }

        public WireConfigSnapshot build() {
            return new WireConfigSnapshot(this);
        }
    }
}
